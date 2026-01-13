from datetime import datetime, timezone
import pandas as pd
from StockDataProviderInterface import StockDataProvider as iStockDataProvider
from yahooquery import Ticker

class StockDataYahooQueryProvider(iStockDataProvider):
  def get_historical_data(self, symbols: list, period: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    tickers = Ticker(symbols, asynchronous=True)
    if period:
      data = tickers.history(period=period)
    elif start and end:
      data = tickers.history(start=start, end=end)
    else:
      raise ValueError("Either 'period' or both 'start' and 'end' must be provided.")
    
    # Format to your standard structure
    return self._format_historical_data(data)
    
  def get_current_prices(self, symbols: list) -> dict:
    tickers = Ticker(symbols, asynchronous=True)
    prices = {}
    for symbol in symbols:
      try:
        price_data = tickers.price.get(symbol, {})
        prices[symbol] = price_data.get('regularMarketPrice')
      except:
        prices[symbol] = None
    return prices
  
  def get_stock_info(self, symbol: str) -> dict:
    ticker = Ticker(symbol)
    return ticker.price.get(symbol, {})
  
  def get_stocks_info(self, symbols: list) -> dict:
    tickers = Ticker(symbols, asynchronous=True)
    info_dict = {}
    for symbol in symbols:
      try:
        info_dict[symbol] = tickers.price[symbol]
      except:
        # Print warning
        print(f"Warning: Could not retrieve info for {symbol}")
        info_dict[symbol] = {}
        
    return info_dict
  
  def _format_historical_data(self, data) -> pd.DataFrame:
    # Your existing formatting logic
    df_formatted = data.reset_index()
    df_formatted = df_formatted[['date', 'symbol', 'adjclose']]

    # Yahoo Query API sometimes returns the 'date' column as a mix of datetime.date and datetime.datetime objects, let's alleviate that!
    for idx, row in df_formatted.iterrows():
      if isinstance(row['date'], datetime):
        df_formatted.at[idx, 'date'] = row['date'].date()

    df_formatted = df_formatted.pivot(index='date', columns='symbol', values='adjclose')
    df_formatted.reset_index(inplace=True)
    df_formatted['date'] = pd.to_datetime(df_formatted['date']).dt.strftime('%Y-%m-%d')
    return df_formatted