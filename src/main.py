# Python packages (in alphabetical order)
import boto3
import json

# Third party packages (in alphabetical order)
import config as cfg
from Emailer import Emailer
from HistDataManager import HistDataManager
from ListManager import ListManager
from ScenarioHandler import ScenarioHandler
from StockDataProviders import StockDataProviders
from StockDataProviderManager import StockDataProviderManager
from StocksManager import StocksManager
from StorageProviders import StorageProviders
from StorageProviderManager import StorageProviderManager

# If for some reason I need to roll back to the PDF logging version, the commit to look for is: 7bd697332aa017d230f2bae1a94e7fd8527b2a68

# TODO For those symbols that don't have after hours, make sure to run another lambda at 6:31am
# TODO Ensure all functions have descriptions
# TODO Implement selenium  webdriver for those that fail with yahoo query or yahoo finance!
# TODO Attempt to download current stocks investments directly from Fidelity using selenium webdriver

def validate_event(event: dict) -> None:
   if 'scenario' not in event:
      raise ValueError("The 'scenario' key is required in the event dictionary.")

    # Ensure scenario is not a falsy value
   if not event['scenario']:
      raise ValueError("The 'scenario' value cannot be empty or falsy.")

def send_email_error(event: dict, E: Exception) -> None:
  emailer = Emailer(cfg.C['email_user'], cfg.C['email_pwd'])
  emailer.set_email_params(
    to=cfg.C['email_to'], 
    subject=f"Error in stocksStats Lambda: {event.get('scenario', 'No scenario provided')}", 
    body=f"An error occurred during the execution of the scenario '{event.get('scenario', 'No scenario provided')}':\n\n{repr(E)}"
  )
  emailer.send()

def lambda_handler(event, context):
  try:
    validate_event(event)

    storage_manager = StorageProviderManager(StorageProviders.AWS_S3)
    yahoo_finance_data_manager = StockDataProviderManager(StockDataProviders.YAHOO_FINANCE)
    yahoo_query_data_manager = StockDataProviderManager(StockDataProviders.YAHOO_QUERY)
    emailer = Emailer(cfg.C['email_user'], cfg.C['email_pwd'])
    scenario = event['scenario']
    s3 = boto3.client('s3')

    list_manager = ListManager(storage_manager, emailer, yahoo_finance_data_manager, yahoo_query_data_manager, cfg.C)
    hist_data_manager = HistDataManager(storage_manager, emailer, yahoo_finance_data_manager, yahoo_query_data_manager, cfg.C)
    # TODO update StocksManager to use `storage_manager` instead of `s3` directly
    # TODO refactor StocksManager logic; OOP; break it into multiple classes; rename maybe to StatsManager or MetricsManager (created issue: https://github.com/muelitas/stocksStats/issues/8)
    stocks_manager = StocksManager(s3, emailer, cfg.C)

    scenario_handler = ScenarioHandler(scenario, list_manager, hist_data_manager, stocks_manager)
    scenario_handler.handle_scenario()

  except Exception as E:
    send_email_error(event, E)
    print(f"Error occurred: {repr(E)}") # Needs this print for when coding locally
    return {
      'statusCode': 500,
      'body': json.dumps(f'Error occurred: {repr(E)}')
    }

  return {
    'statusCode': 200,
    'body': json.dumps('Hello from Lambda that was updated through CodeBuild.')
  }

if __name__ == "__main__":
   # TODO check how we are able to import from modules in a lambda in Nexus
   lambda_handler(None, None)
    # List Manager scenarios:
    # lambda_handler({"scenario": "upsert_stocks_list"}, None)

    # Hist Data Manager scenarios:
    # lambda_handler({"scenario": "upsert_historical_data"}, None)

    # Stocks Manager scenarios:
    # lambda_handler({"scenario": "update_last_closing_price"}, None)
    # lambda_handler({"scenario": "analyze_pre_market_prices"}, None)