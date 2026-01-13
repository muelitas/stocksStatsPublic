import sys
import time

import pandas as pd
 # TODO include screeners logic in Stock Data Providers or create its own module (created issue: https://github.com/muelitas/stocksStats/issues/5)
from yahooquery import Screener
from StockDataProviderManager import StockDataProviderManager
from StorageProviderManager import StorageProviderManager

from Emailer import Emailer

class ListManager:
  def __init__(self, storage_manager: StorageProviderManager, emailer: Emailer, yahoo_finance_data_manager: StockDataProviderManager, yahoo_query_data_manager: StockDataProviderManager, cfg: dict):
    self.s3_mgr = storage_manager
    self.emailer = emailer
    self.yfinance_manager = yahoo_finance_data_manager
    self.yquery_manager = yahoo_query_data_manager
    # TODO once config is defined, validate it here and create class attributes
    self.cfg = cfg

    self.warnings = []

    self.in_update_mode = None

  #region Checks
  def __check_for_list_uniqueness(self, lst: list, obj_key: str) -> None:
    if len(lst) != len(set(lst)):
      warning_msg = f"Warning: The list in {obj_key} contains duplicate entries."
      print(warning_msg)
      self.warnings.append(warning_msg)

  def __check_for_stocks_overlap(self, nasdaq_and_nyse: pd.DataFrame, otc: pd.DataFrame, currently_invested: pd.DataFrame) -> None:
    otc_nasdaq_and_nyse = set(otc['symbol']).intersection(set(nasdaq_and_nyse['symbol']))
    if otc_nasdaq_and_nyse:
      print(f"\tWarning: The following stocks are in both otc and nasdaq_and_nyse: {otc_nasdaq_and_nyse}")
      self.warnings.append(f"The following stocks are in both otc and nasdaq_and_nyse: {otc_nasdaq_and_nyse}")

    otc_currently_invested = set(otc['symbol']).intersection(set(currently_invested['symbol']))
    if otc_currently_invested:
      print(f"\tWarning: The following stocks are in both otc and currently_invested: {otc_currently_invested}")
      self.warnings.append(f"The following stocks are in both otc and currently_invested: {otc_currently_invested}")

    nasdaq_and_nyse_vs_currently_invested = set(nasdaq_and_nyse['symbol']).intersection(set(currently_invested['symbol']))
    if nasdaq_and_nyse_vs_currently_invested:
      print(f"\tWarning: The following stocks are in both nasdaq_and_nyse and currently_invested: {nasdaq_and_nyse_vs_currently_invested}")
      self.warnings.append(f"The following stocks are in both nasdaq_and_nyse and currently_invested: {nasdaq_and_nyse_vs_currently_invested}")

    # Which stocks are in currently_invested but not in nasdaq_and_nyse or otc
    currently_invested_not_in_others = set(currently_invested['symbol']).difference(set(nasdaq_and_nyse['symbol']).union(set(otc['symbol'])))
    if currently_invested_not_in_others:
      print(f"\tWarning: The following stocks are in currently_invested but not in nasdaq_and_nyse or otc: {currently_invested_not_in_others}")
      self.warnings.append(f"The following stocks are in currently_invested but not in nasdaq_and_nyse or otc: {currently_invested_not_in_others}")

  def __check_fresh_vs_old_difference(self, fresh_stocks: set, old_stocks: set) -> None:
    # If a stock in ground truth is not in fresh stocks, it means it was removed by screeners
    removed_stocks = old_stocks.difference(fresh_stocks)
    if removed_stocks:
      # TODO find a reason for each removed stock
      print(f"\tWarning: The following stocks were removed by screeners: {removed_stocks}")
      self.warnings.append(f"The following stocks were removed by screeners: {removed_stocks}")

  #endregion

  #region Gets  
  def __get_currently_invested_stocks(self) -> pd.DataFrame:
    stocks_list = self.s3_mgr.read(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_currently_invested_stocks_txt_name'])
    self.__check_for_list_uniqueness(stocks_list, self.cfg['s3_currently_invested_stocks_txt_name'])

    # If stocks list is greater than 100, raise a warning
    if len(stocks_list) > 100:
      warning_msg = f"Warning: The list in {self.cfg['s3_currently_invested_stocks_txt_name']} contains more than 100 entries. Consider alternating between APIs."
      print(warning_msg)
      self.warnings.append(warning_msg)

    batch_size = 20
    batches = [stocks_list[i:i + batch_size] for i in range(0, len(stocks_list), batch_size)]

    stocks_and_info = []
    stocks_set = set()
    # TODO Same here, we would benefit from a class that handles multiple stock data providers usage (so it helps prevent hitting rate limits) (created issue: https://github.com/muelitas/stocksStats/issues/6)
    print(f"Getting stocks from Currently Invested list")
    for i, batch in enumerate(batches):
      print(f"\tProcessing batch {i + 1} of {len(batches)}")
      tickers_info = self.yquery_manager.get_stocks_info(batch)
      for symbol in batch:
        info = tickers_info.get(symbol, {})
        if 'marketCap' not in info:
          print(f"\t\tSkipping symbol {symbol} as it has no marketCap info")
          continue

        if symbol in stocks_set:
          continue  # Skip duplicates

        stocks_set.add(symbol)
        stock_data = {
            'symbol': symbol,
            'marketCap': info['marketCap'],
            'exchange': info['exchange'] if 'exchange' in info else 'N/A',
            'fullExchangeName': info['fullExchangeName'] if 'fullExchangeName' in info else 'N/A',
            'hasPreMarketData': info['preMarketPrice'] is not None if 'preMarketPrice' in info else False,
            'screener': 'N/A'
        }

        stocks_and_info.append(stock_data)

    print(f"\tTotal unique Currently Invested stocks: {len(stocks_and_info)}")
    # Make the list a dataframe
    stocks_df = pd.DataFrame(stocks_and_info)
    return stocks_df

  def __get_otc_stocks(self) -> pd.DataFrame:
    # I ended up using this: https://stockanalysis.com/stocks/screener/ and manually downloading HTML then converting to CSV; TODO find a way to automate this
    stocks_list = self.s3_mgr.read(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_otc_stocks_txt_name'])
    self.__check_for_list_uniqueness(stocks_list, self.cfg['s3_otc_stocks_txt_name'])

    # Use Ticker from yahooquery to get more info about each stock
    batch_size = 20
    batches = [stocks_list[i:i + batch_size] for i in range(0, len(stocks_list), batch_size)]

    stocks_and_info = []
    stocks_set = set()
    stocks_below_market_cap_threshold = []
    print(f"Getting stocks from OTC Markets list")
    # TODO create a class that handles multiple stock data providers usage (so it helps prevent hitting rate limits) and implement it here (created issue: https://github.com/muelitas/stocksStats/issues/6)
    for i, batch in enumerate(batches):
      print(f"\tProcessing batch {i + 1} of {len(batches)}")
      tickers_info = self.yquery_manager.get_stocks_info(batch)
      for symbol in batch:
        info = tickers_info.get(symbol, {})
        if 'marketCap' not in info:
          print(f"\t\tSkipping symbol {symbol} as it has no marketCap info")
          continue

        if symbol in stocks_set:
          continue  # Skip duplicates

        if info['marketCap'] < self.cfg['market_cap_threshold']:
          # print(f"\t\tSymbol {symbol} has marketCap {info['marketCap']} which is below the threshold of {self.cfg['market_cap_threshold']}")
          stocks_below_market_cap_threshold.append(symbol)

        stocks_set.add(symbol)
        stock_data = {
            'symbol': symbol,
            'marketCap': info['marketCap'],
            'exchange': info['exchange'] if 'exchange' in info else 'N/A',
            'fullExchangeName': info['fullExchangeName'] if 'fullExchangeName' in info else 'N/A',
            'hasPreMarketData': info['preMarketPrice'] is not None if 'preMarketPrice' in info else False,
            'screener': 'N/A'
        }

        stocks_and_info.append(stock_data)

    print(f"\tThese {len(stocks_below_market_cap_threshold)} stocks were below market cap threshold ({self.cfg['market_cap_threshold']}): {stocks_below_market_cap_threshold}")
    print(f"\tTotal unique OTC stocks: {len(stocks_and_info)}")

    # Make the list a dataframe
    stocks_df = pd.DataFrame(stocks_and_info)
    return stocks_df

  def __get_nyse_and_nasdaq_stocks(self) -> pd.DataFrame:
    """
    Get stocks data from NYSE and NASDAQ screeners defined in S3 file
    """
    screeners = self.s3_mgr.read(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_screeners_file_name'])

    stocks = []
    _stocks_set = set()
    no_market_cap_stocks = []
    print(f"Getting stocks from screeners (NYSE and NASDAQ)")
    for screener_name in screeners:
      s = Screener() # TODO include screeners logic in Stock Data Providers or create its own module (created issue: https://github.com/muelitas/stocksStats/issues/5)
      result = s.get_screeners(screener_name, count=self.cfg['symbols_per_screener'])
      if result[screener_name] == 'No screener records found. Check if scrIds and marketRegion combination are correct':
        print(f"\tSkipping screener {screener_name} as it returned no records")
        continue

      for idx, i in enumerate(result[screener_name]['quotes']):
        if 'symbol' not in i:
          print(f"\tSkipping entry {idx} as it has no symbol")
          continue
        
        if 'marketCap' not in i:
          no_market_cap_stocks.append(i['symbol'])
          # print(f"\tSkipping entry {idx} ({i['symbol']}) as it has no marketCap")
          continue

        if i['marketCap'] < self.cfg['market_cap_threshold']:
            continue
        
        if i['symbol'] in _stocks_set:
          continue  # Skip duplicates

        _stocks_set.add(i['symbol'])
        stock_data = {
          'symbol': i['symbol'],
          'marketCap': i['marketCap'],
          'exchange': i['exchange'] if 'exchange' in i else 'N/A',
          'fullExchangeName': i['fullExchangeName'] if i['fullExchangeName'] is not None else 'N/A',
          'hasPreMarketData': i['preMarketPrice'] is not None if 'preMarketPrice' in i else False,
          'screener': screener_name
        }

        stocks.append(stock_data)

    # Make the list a dataframe
    stocks_df = pd.DataFrame(stocks)
    print(f"\tThese {len(no_market_cap_stocks)} stocks had no market cap info (skipped): {no_market_cap_stocks}")
    print(f"\tTotal unique NYSE and NASDAQ stocks: {stocks_df.shape[0]}")
    return stocks_df

  #endregion

  #region Update
  def __send_successful_update_email(self, msg: str) -> None:
    body = f"The stocks list was successfully updated.\n\n{msg}"
    if self.warnings:
      body += "\n\nWarnings:\n" + "\n".join(self.warnings)

    self.emailer.set_email_params(
      to=self.cfg['email_to'], 
      subject="Stocks List Updated", 
      body=body
    )
    self.emailer.send()

  def __save_new_stocks_list_to_s3(self, stocks_df: pd.DataFrame) -> None:
    # Sort and save the new ground truth
    new_ground_truth_stocks = stocks_df.sort_values(by='symbol')
    self.s3_mgr.create(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_all_stocks_csv_name'], data=new_ground_truth_stocks)

  def __update(self) -> None:
    # Raise an error as this needs to be updated
    raise NotImplementedError("Update method not implemented")

    stocks_list_ground_truth = self.__read_txt_file_from_s3(self.cfg['s3_bucket'], self.cfg['s3_all_stocks_file_name'])
    print(f"\tGround truth list has {len(stocks_list_ground_truth)} stocks")
    stocks_from_screeners = self.__get_nyse_and_nasdaq_stocks()
    print(f"\tScreeners produced {len(stocks_from_screeners)} unique stocks.")

    nasdaq_otc_and_others_stocks = self.__read_txt_file_from_s3(self.cfg['s3_bucket'], self.cfg['s3_nasdaq_otc_and_others_file_name'])
    print(f"\tManual file returned {len(nasdaq_otc_and_others_stocks)} unique stocks.")
    
    self.__check_for_stocks_overlap(stocks_from_screeners, nasdaq_otc_and_others_stocks)
    
    fresh_stocks_union = set(stocks_from_screeners).union(set(nasdaq_otc_and_others_stocks))
    self.__check_fresh_vs_old_difference(fresh_stocks_union, set(stocks_list_ground_truth))

    new_ground_truth_stocks = list(fresh_stocks_union.union(set(stocks_list_ground_truth)))
    self.__save_new_stocks_list_to_s3(new_ground_truth_stocks)
    msg = f"{len(new_ground_truth_stocks)} stocks saved to {self.cfg['s3_all_stocks_file_name']} in bucket {self.cfg['s3_bucket']}"
    print(msg)
    self.__send_successful_update_email(msg)

  #endregion

  #region Create
  def __create(self) -> None:
    """
    Create a new stocks list by merging NYSE and NASDAQ stocks, OTC stocks, and currently invested stocks.
    """
    nyse_and_nasdaq_stocks_df = self.__get_nyse_and_nasdaq_stocks()
    otc_stocks_df = self.__get_otc_stocks()
    currently_invested_stocks_df = self.__get_currently_invested_stocks()

    self.__check_for_stocks_overlap(nyse_and_nasdaq_stocks_df, otc_stocks_df, currently_invested_stocks_df)

    # Concatenate the dataframes, ensure no duplicates on the 'symbol' column
    merged_stocks_df = pd.concat([nyse_and_nasdaq_stocks_df, otc_stocks_df, currently_invested_stocks_df]).drop_duplicates(subset='symbol')

    self.__save_new_stocks_list_to_s3(merged_stocks_df)
    msg = f"{merged_stocks_df.shape[0]} stocks saved to {self.cfg['s3_all_stocks_csv_name']} in bucket {self.cfg['s3_bucket']}"
    print(msg)
    self.__send_successful_update_email(msg)

  #endregion

  #region Upsert
  def __send_failed_upsert_email(self, error: Exception) -> None:
    in_update_mode = self.in_update_mode
    mode = "update" if in_update_mode else "create" if in_update_mode == False else "upsert"
    body = f"""
    An error occurred while trying to {mode} the stocks list.

    Error details:
    {repr(error)}

    Warnings:
    """
    for warning in self.warnings:
      body += f"- {warning}\n"

    self.emailer.set_email_params(
      to=self.cfg['email_to'], 
      subject=f"Failed to {mode} stocks list", 
      body=body
    )

    self.emailer.send()

  def upsert(self) -> None:
    # TODO update the update logic (created issue: https://github.com/muelitas/stocksStats/issues/7) 
    self.__create()
    sys.exit()
    # TODO if this file doesn't run on a weekday during pre market hours, raise an error since we are storing 'hasPreMarketData' in the stocks list
    try:
      self.in_update_mode = self.s3_mgr.check_existence(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_all_stocks_csv_name'])
      if self.in_update_mode:
        print("In Update mode")
        self.__update()
      else:
        print("In Create mode")
        self.__create()
    except Exception as E:
      self.__send_failed_upsert_email(E)
      print(f"An error occurred while upserting the stocks list: {repr(E)}")
      # No need to raise the error since we will be sending an email
    
  #endregion
