from StockDataProviders import StockDataProviders
import pandas as pd
from StockDataFactory import StockDataFactory

class StockDataProviderManager:
  """Main class that uses the factory and provides a unified interface"""
  
  def __init__(self, provider: StockDataProviders):
    self.provider = StockDataFactory.create_provider(provider)

  def get_historical_data(self, symbols: list, period: str = None, start: str = None, end: str = None, **kwargs) -> pd.DataFrame:
    try:
      return self.provider.get_historical_data(symbols, period=period, start=start, end=end, **kwargs)
    except Exception as e:
      raise e
  
  def get_current_prices(self, symbols: list) -> dict:  
    try:
      return self.provider.get_current_prices(symbols)
    except Exception as e:
      raise e
    
  def get_stock_info(self, symbol: str) -> dict:
    try:
      return self.provider.get_stock_info(symbol)
    except Exception as e:
      raise e
    
  def get_stocks_info(self, symbols: list) -> dict:
    try:
      return self.provider.get_stocks_info(symbols)
    except Exception as e:
      raise e