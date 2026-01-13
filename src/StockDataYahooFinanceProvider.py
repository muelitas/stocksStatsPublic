import pandas as pd
from StockDataProviderInterface import StockDataProvider as iStockDataProvider
import yfinance as yf

class StockDataYahooFinanceProvider(iStockDataProvider):
  def get_historical_data(self, symbols: list, period: str = None, start: str = None, end: str = None) -> pd.DataFrame:
    if period:
      data = yf.download(symbols, period=period, group_by='ticker')
    elif start and end:
      data = yf.download(symbols, start=start, end=end, group_by='ticker')
    else:
      raise ValueError("Either 'period' or both 'start' and 'end' must be provided.")
    
    # Format to your standard structure
    return self._format_historical_data(data, symbols)
  
  def get_current_prices(self, symbols: list) -> dict:
    tickers = yf.Tickers(' '.join(symbols))
    prices = {}
    for symbol in symbols:
      try:
        info = tickers.tickers[symbol].info
        prices[symbol] = info.get('regularMarketPrice')
      except:
        prices[symbol] = None
    return prices
  
  def get_stock_info(self, symbol: str) -> dict:
    ticker = yf.Ticker(symbol)
    return ticker.info
  
  def get_stocks_info(self, symbols: list) -> dict:
    tickers = yf.Tickers(' '.join(symbols))
    info_dict = {}
    for symbol in symbols:
      try:
        info_dict[symbol] = tickers.tickers[symbol].info
      except:
        # Print warning
        print(f"Warning: Could not retrieve info for {symbol}")
        info_dict[symbol] = {}

    return info_dict
  
  def _format_historical_data(self, h_data, symbols) -> pd.DataFrame:
    # h_data is a multi-index DataFrame when multiple symbols are provided
    # We will reformat it to have Date as index and each symbol's Close price as a column
    df_list = []
    for ticker in symbols:
      df_ticker = h_data[ticker][['Close']].copy()
      df_ticker.rename(columns={'Close': ticker}, inplace=True)
      df_ticker.reset_index(inplace=True)
      df_list.append(df_ticker)

    # Now let's merge all dataframes in df_list on the Date column
    df_merged = df_list[0]
    for df in df_list[1:]:
      df_merged = pd.merge(df_merged, df, on='Date', how='outer')

    df_merged['Date'] = pd.to_datetime(df_merged['Date']).dt.strftime('%Y-%m-%d')
    # Change the column name 'Date' to 'date' to maintain consistency
    df_merged.rename(columns={'Date': 'date'}, inplace=True)
    return df_merged