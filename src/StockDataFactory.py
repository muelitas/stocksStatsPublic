from StockDataProviders import StockDataProviders
from StockDataProviderInterface import StockDataProvider as iStockDataProvider
from StockDataYahooFinanceProvider import StockDataYahooFinanceProvider
from StockDataYahooQueryProvider import StockDataYahooQueryProvider

class StockDataFactory:
  """Factory class to create stock data providers"""
  
  @staticmethod
  def create_provider(api_type: StockDataProviders) -> iStockDataProvider:
    if api_type == StockDataProviders.YAHOO_FINANCE:
      return StockDataYahooFinanceProvider()
    elif api_type == StockDataProviders.YAHOO_QUERY:
      return StockDataYahooQueryProvider()
    else:
      raise ValueError(f"Unsupported API type: {api_type}")