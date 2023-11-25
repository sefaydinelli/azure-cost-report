import datetime
import logging
from azure.identity import DefaultAzureCredential
from azure.mgmt.costmanagement import CostManagementClient
import json
import requests
import os

import azure.functions as func

def get_cost_and_usage(time_period_start, time_period_end, subscription_id, cost_client):
  cost_data = {}
  query = {
    'type': 'Usage',
    'timeframe': 'Custom',
    'time_period': {'from': time_period_start, 'to': time_period_end},
    'dataset': {
      'granularity': 'Daily',
      'grouping': [{'type': 'Dimension', 'name': 'ResourceGroup'}],
      'aggregation': {
        'totalCost': {'name': 'PreTaxCost', 'function': 'Sum'}
      }
    }
  }

  scope = f"/subscriptions/{subscription_id}"

  try:
    result = cost_client.query.usage(scope=scope, parameters=query)
    logging.info(result)
    if result and result.rows:
      for row in result.rows:
        date = row[1]
        cost = float(row[0])
        if row[2] == '':
          rg_name = 'No Resource Group Name'
        else:
          rg_name = row[2]
        
        if date not in cost_data:
          cost_data[date] = {}
        cost_data[date][rg_name] = cost
  except Exception as e:
    print(f"Error: {e}")

  return cost_data

def sort_cost_data(cost_data):
  sorted_cost_data = {}
  for date in sorted(cost_data.keys()):
    sorted_cost_data[date] = {}
    for rg in sorted(cost_data[date].keys(), key=lambda x: cost_data[date][x], reverse=True):
      sorted_cost_data[date][rg] = cost_data[date][rg]
  return sorted_cost_data

def convert_date_data(cost_data):
  converted_cost_data = {}
  for date in cost_data.keys():
    converted_cost_data[datetime.datetime.strptime(str(date), '%Y%m%d').strftime('%d/%m/%Y')] = cost_data[date]
  return converted_cost_data

def add_total_cost(cost_data):
  for date in cost_data.keys():
    total_cost = 0
    for rg in cost_data[date].keys():
      total_cost += cost_data[date][rg]
    cost_data[date]['Total Daily Cost'] = round(total_cost, 2)
  return cost_data

def format_slack_message(cost_data):
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Azure Daily Cost Report"
            }
        },
        {"type": "divider"}
    ]

    dates = list(cost_data.keys())
    costs = [cost_data[date].get('Total Daily Cost', 0) for date in dates]

    for i, date in enumerate(dates):
        fields = [
            {"type": "mrkdwn", "text": f"*Date*: {date}"},
            {"type": "mrkdwn", "text": f"*Total Cost*: {costs[i]}€"}
        ]

        if i > 0:
            diff = costs[i] - costs[i - 1]
            diff_sign = "+" if diff >= 0 else ""
            emoji = ":large_red_square:" if diff >= 0 else ":large_green_square:"
            fields.append({
                "type": "mrkdwn",
                "text": f"*Daily Difference*: {diff_sign}{diff:.2f}€ {emoji}"
            })

        blocks.append({
            "type": "section",
            "fields": fields
        })

        rg_costs = ""
        for rg in list(cost_data[date].keys())[0:5]:
            rg_costs += f"{rg}: {cost_data[date][rg]:.2f}€ \n"

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Top 5 Resource Group Costs:*\n{rg_costs}"
            }
        })

        blocks.append({"type": "divider"})

    return {"blocks": blocks}

def send_slack_message(blocks, channel_name, slack_webhook):
    slack_data = {
        "channel": channel_name,
        "username": "Cost Report Bot",
        "icon_emoji": ":money_with_wings:",
        "blocks": blocks
    }

    response = requests.post(
        slack_webhook, json=slack_data,
        headers={'Content-Type': 'application/json'}
    )

    if response.status_code != 200:
        raise ValueError(
            f"Request to slack returned an error {response.status_code}, "
            f"the response is:\n{response.text}"
        )

def main(dailyTrigger: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if dailyTrigger.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    credential = DefaultAzureCredential()
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    cost_client = CostManagementClient(credential, subscription_id=subscription_id)
    slack_webhook = os.environ["SLACK_WEBHOOK"]
    slack_channel_name = os.environ["SLACK_CHANNEL_NAME"]

    time_period_start = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=4), '%Y-%m-%dT00:00:00Z')
    time_period_end = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(days=2), '%Y-%m-%dT23:59:59Z')

    cost_data = get_cost_and_usage(time_period_start, time_period_end, subscription_id, cost_client)
    sorted_cost_data = sort_cost_data(cost_data)
    date_converted_cost_data = convert_date_data(sorted_cost_data)
    total_cost_data = add_total_cost(date_converted_cost_data)

    slack_message_content = format_slack_message(total_cost_data)
    send_slack_message(slack_message_content['blocks'], slack_channel_name, slack_webhook)
