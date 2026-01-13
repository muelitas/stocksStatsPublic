from HistDataManager import HistDataManager
from ListManager import ListManager
from StocksManager import StocksManager

# Might be overkill to have a whole class for this, but it keeps main.py cleaner
class ScenarioHandler:
  def __init__(self, scenario: str, list_manager: ListManager, hist_data_manager: HistDataManager, stocks_manager: StocksManager) -> None:
    self.scenario = scenario
    self.list_manager = list_manager
    self.hist_data_manager = hist_data_manager
    self.stocks_manager = stocks_manager

    self.valid_scenario_keys = [
      "upsert_stocks_list", # TODO run it on the first saturday of every month
      "upsert_historical_data", # TODO run it on the first sunday of every month
      # TODO create a scenario in which I can check/validate historical data, maybe run it on the second and fourth sunday of every month
      "update_last_closing_price", # Runs on weekdays at 6:30pm
      "analyze_pre_market_prices", # Runs on weekdays at 6:24am
      "regular_market_processing" # TODO run it on weekdays at 10:31am (mid day analysis)
    ]

  def handle_scenario(self) -> None:
    print(f"Handling scenario: {self.scenario}")
    self.__validate_scenario()
    self.__execute_scenario()

  def __execute_scenario(self) -> None:
    if self.scenario == "upsert_stocks_list":
      # TODO here, I need to see if there is a way to see when the stock was founded, if not longer than 1 year, remove it from the list
      self.list_manager.upsert()
    elif self.scenario == "upsert_historical_data":
      self.hist_data_manager.upsert()
    elif self.scenario == "update_last_closing_price":
      self.stocks_manager.update_last_closing_price()
    elif self.scenario == "analyze_pre_market_prices":
      self.stocks_manager.analyze_pre_market_prices()
    elif self.scenario == "regular_market_processing":
      # Raise a not implemented error for now
      raise NotImplementedError("The 'regular_market_processing' scenario is not yet implemented.")
    else:
      # This should never happen due to prior validation
      raise ValueError(f"Unhandled scenario: {self.scenario}")

  def __validate_scenario(self) -> None:
    if self.scenario not in self.valid_scenario_keys:
      raise ValueError(f"Invalid scenario: {self.scenario}. Valid scenarios are: {self.valid_scenario_keys}")