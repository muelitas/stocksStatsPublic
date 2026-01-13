from datetime import date, timedelta, datetime, time
import io
import math
import os
import pytz
import sys

import boto3
import botocore
import pandas as pd
from yahooquery import Screener, Ticker

from Emailer import Emailer

class StocksManager:
  def __init__(self, s3_client: boto3.client, emailer: Emailer, cfg: dict):
    self.s3_client = s3_client
    self.emailer = emailer
    # TODO once config is defined, validate it here and create class attributes
    self.cfg = cfg

    self.__hdata = None
    self.infos = []
    self.warnings = []

    # PDF font sizes
    self.metricValueFontSize = 9
    self.metricLabelFontSize = 7

  # Short for Print and Warn
  def paw(self, msg: str) -> None:
    print(msg)
    self.warnings.append(msg)

  # Short for Print and Info
  def pai(self, msg: str) -> None:
    print(msg)
    self.infos.append(msg)

  #region Checks
  def __check_s3_object_exists(self, bucket_name, key):
    """
    Checks if an object exists in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The key (path) of the object in the S3 bucket.

    Returns:
        bool: True if the object exists, False otherwise.
    """
    try:
        self.s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # Handle other potential errors (e.g., permissions)
            print(f"An error occurred: {e}")
            raise e
        
  def __check_closing_prices_are_equal(self, closing_prices_sum: float) -> None:
    stocks_in_hd = self.__hdata.columns[self.__hdata.columns != 'date'].tolist()
    last_row = self.__hdata.iloc[-1]
    hd_closing_prices_sum = last_row[stocks_in_hd].sum()

    self.pai(f"Sum of last closing prices from screeners and Ticker: {closing_prices_sum}")
    self.pai(f"Sum of last closing prices from historical data: {hd_closing_prices_sum}")
    self.pai(f"Difference: {abs(closing_prices_sum - hd_closing_prices_sum)}")
    
    # If the difference is less than 2, we can skip the operation, because chances are that we already updated it and we are on the weekend or a holiday, so the prices are the same
    threshold = 2
    if abs(closing_prices_sum - hd_closing_prices_sum) < threshold:
      msg = f"The difference between the sum of last closing prices from screeners and Ticker ({closing_prices_sum}) and the sum from the historical data ({hd_closing_prices_sum}) is less than {threshold}. Assuming prices are the same and skipping update."
      self.pai(msg)
      return True
    
    return False
    
  #endregion

  #region Gets
  def __get_last_closing_prices_and_sum(self, stocks_and_info: dict) -> tuple[dict, float]:
    stocks_in_hd = self.__hdata.columns[self.__hdata.columns != 'date'].tolist()
    last_closing_prices = {}
    closing_prices_sum = 0
    for symbol in stocks_in_hd:
      if 'regularMarketPrice' not in stocks_and_info.get(symbol, {}):
        msg = f"Could not find regularMarketPrice for stock {symbol}. Removing it from historical data."
        self.paw(msg)

        # Remove the stock from the historical data dataframe self.__hdata
        self.__hdata = self.__hdata.drop(columns=[symbol])
        continue

      last_closing_prices[symbol] = stocks_and_info[symbol]['regularMarketPrice']
      closing_prices_sum += last_closing_prices[symbol]
    
    return last_closing_prices, closing_prices_sum

  def __get_hist_data_from_s3(self, obj_key: str) -> None:
    response = self.s3_client.get_object(Bucket=self.cfg['s3_bucket'], Key=obj_key)
    body = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(body))
    start_date, end_date = df['date'].min(), df['date'].max()

    self.pai("Successfully downloaded the historical data.")
    self.pai(f"It has {df.shape[0]} rows and {df.shape[1]} columns")
    self.pai(f"It ranges from {start_date} to {end_date}")
    self.__hdata = df
  
  def __get_screeners_list_from_s3(self) -> list:
    response = self.s3_client.get_object(Bucket=self.cfg['s3_bucket'], Key=self.cfg['s3_screeners_file_name'])
    body = response['Body'].read().decode('utf-8')
    screeners_list = body.splitlines()
    return screeners_list
  
  # NOTE :This method is different than the one in ListManager since here I need more info about the stocks (consider merging both methods later and adding fields masking)
  def __get_stocks_list_from_screeners(self) -> dict:
    screeners = self.__get_screeners_list_from_s3()

    stocks = {}
    # TODO add these attrs to the config file
    symbols_per_screener_limit = 250 # 250 seems to be the max you can get
    market_cap_threshold = 2_000_000_000  # $2 billion

    for screener_name in screeners:
      s = Screener()
      result = s.get_screeners(screener_name, count=symbols_per_screener_limit)
      if result[screener_name] == 'No screener records found. Check if scrIds and marketRegion combination are correct':
        print(f"\tSkipping screener {screener_name} as it returned no records")
        continue

      for idx, i in enumerate(result[screener_name]['quotes']):
        if 'symbol' not in i:
          print(f"\tSkipping entry {idx} as it has no symbol")
          continue

        if 'marketCap' not in i:
          print(f"\tSkipping entry {idx} ({i['symbol']}) as it has no marketCap")
          continue

        if i['marketCap'] < market_cap_threshold:
            continue

        stocks[i['symbol']] = i

    self.pai(f"Screeners returned {len(stocks)} unique stocks.")
    return stocks
  
  def __get_missing_stocks_info(self, stocks_from_screeners: dict) -> None:
    stocks_in_hd = self.__hdata.columns[self.__hdata.columns != 'date'].tolist()
    # Find which stock symbols are missing from the screeners list by comparing it to the historical data (since historical data should have a more complete list)
    missing_stocks = set(stocks_in_hd) - set(stocks_from_screeners.keys())
    
    if not missing_stocks:
      self.pai("All stocks in the historical data are present in the screeners list.")
      return

    # Lets get the info for the missing stocks using Ticker
    self.pai(f"There are {len(missing_stocks)} stocks in the historical data that are not in the screeners list: {missing_stocks}")
    # TODO if more than 190 tickers, need to split the list and do multiple calls, create a class that handles this (created issue: https://github.com/muelitas/stocksStats/issues/6)
    tickers = Ticker(list(missing_stocks), asynchronous=True)
    for symbol in missing_stocks:
      info = tickers.price.get(symbol, {})
      if info:
        stocks_from_screeners[symbol] = info
      else:
        msg = f"Could not find info for stock {symbol} using Ticker. Removing it from historical data."
        self.paw(msg)

        # Remove the stock from the historical data dataframe self.__hdata
        self.__hdata = self.__hdata.drop(columns=[symbol])

  def __get_surface_area_ratio(self, h_data: pd.Series, curr_price: float) -> float:
    """The idea here is to first find the min value of data and curr price. We will use this to replace the "zero" base.
        In other words, we subtract all data by this min value so that our new zero is at the min value. Since all values in
        historical data are shifted down, we need to shift down the curr price as well. We then grab all those values that
        are above the curr price. Since we are trying to get the surface area above the curr price, we add all of these values
        up and for each value we subtract the curr price. Then, to calcualte the below surface area, we add all the values that
        live underneath the current price and add the curr price n times (where n is the number of values that had a value above
        the curr price).
    
    Parameters
    ----------
        h_data : Dataframe
            stock's historical data
        curr_price: Float
            stock's price at the market (at regular, pre or post market hours, depending on the current time)

    Returns
    -------
        float
            A ratio representing surface area above curr price line divided by the surface area that lays underneath
    """
    min_in_data = h_data.min()
    overall_min = min(min_in_data, curr_price)
    data_minus_min = h_data - overall_min
    curr_price_minus_min = curr_price - overall_min
    vals_above_curr_price = data_minus_min[data_minus_min > curr_price_minus_min]
    vals_above_curr_price_sum = vals_above_curr_price.sum() - (len(vals_above_curr_price) * curr_price_minus_min)
    vals_below_curr_price_sum = data_minus_min[data_minus_min < curr_price_minus_min].sum() + (len(vals_above_curr_price) * curr_price_minus_min)

    if vals_below_curr_price_sum == 0:
      return math.inf
    
    return round(vals_above_curr_price_sum/vals_below_curr_price_sum, 3)

  #endregion

  #region LastClosePriz
  def __add_last_closing_price_to_historical_data(self, closing_prices: dict) -> None:
    # Print the last three rows of the historical data, for the first 5 columns
    self.pai("First 3 rows of historical data (before update):")
    self.pai(self.__hdata.iloc[-3:, :5].to_string(index=False))

    # Print the first three rows of the historical data, for the first 5 columns
    self.pai("Last 3 rows of historical data (before update):")
    self.pai(self.__hdata.iloc[:3, :5].to_string(index=False))

    # We want to add a new row to the historical data with the last closing prices
    today = date.today().strftime('%Y-%m-%d')
    new_row = {'date': today}
    stocks_in_hd = self.__hdata.columns[self.__hdata.columns != 'date'].tolist()
    for symbol in stocks_in_hd:
      new_row[symbol] = closing_prices.get(symbol)

    df = pd.concat([self.__hdata, pd.DataFrame([new_row])], ignore_index=True)

    # Remove the first row to keep the size of the historical data the same
    df = df.iloc[1:, :]
  
    self.__hdata = df
    # Print the last three rows of the historical data, for the first 5 columns
    self.pai("First 3 rows of historical data (after update):")
    self.pai(self.__hdata.iloc[:3, :5].to_string(index=False))

    # Print the last three rows of the historical data, for the first 5 columns
    self.pai("Last 3 rows of historical data (after update):")
    self.pai(self.__hdata.iloc[-3:, :5].to_string(index=False))

    # Print the shape of the dataframe
    start_date, end_date = df['date'].min(), df['date'].max()
    self.pai(f"Now, the historical data has {df.shape[0]} rows and {df.shape[1]} columns")
    self.pai(f"The historical data date range is from {start_date} to {end_date}")

  def __update_last_closing_price_checks(self) -> None:
    # Check if today is a weekday
    if date.today().weekday() > 4: # Mon-Fri are 0-4
      raise ValueError("Today is not a weekday. Cannot update last closing prices on weekends.")

    # If the time is not between 1:30 PM and 11:30 PM Pacific Time, raise an error
    current_time = datetime.now(pytz.timezone('America/Los_Angeles')).time()
    if not (time(13, 30) <= current_time <= time(23, 30)):
      raise ValueError("Current time is not between 1:30 PM and 11:30 PM Pacific Time. Cannot update last closing prices outside of this time range.")
    
    # Check if historical data exists
    if not self.__check_s3_object_exists(self.cfg['s3_bucket'], self.cfg['s3_historical_data_csv_name']):
      raise FileNotFoundError(f"The historical data file {self.cfg['s3_historical_data_csv_name']} does not exist in bucket {self.cfg['s3_bucket']}. Cannot update last closing prices.")
      
  def __upload_new_historical_data_to_s3(self, df: pd.DataFrame) -> None:
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    self.s3_client.put_object(Bucket=self.cfg['s3_bucket'], Key=self.cfg['s3_historical_data_csv_name'], Body=csv_buffer.getvalue())
    csv_buffer.close()
    self.pai("Successfully uploaded the updated historical data.")
    self.pai(f"It now has {df.shape[0]} rows and {df.shape[1]} columns")

  def __send_last_closing_price_update_email(self) -> None:
    body = f"The last closing prices were successfully processed.\n\n"

    if self.warnings:
      body += "\nWarnings:\n"
      for warning in self.warnings:
        body += f"- {warning}\n"

    if self.infos:
      body += "\n\nInfos:\n"
      for info in self.infos:
        body += f"- {info}\n"

    self.emailer.set_email_params(
      to=self.cfg['email_to'], 
      subject="Last Closing Prices Processed", 
      body=body
    )

    self.emailer.send()

  def update_last_closing_price(self) -> None:
    '''Steps:
    1. Download the historical data file from S3
    2. For each stock in the historical data file, get the last closing price from Yahoo Query
    2.1 Use the screeners first, if not found, use Ticker
    2.2 If still not found, log a warning and move on to the next
    3. Update the last closing price in the historical data file
    '''
    try:
      # Run checks
      self.__update_last_closing_price_checks()
      
      # Download historical data
      self.__get_hist_data_from_s3(self.cfg['s3_historical_data_csv_name'])
      
      # Get stocks, and their financial data, from screeners
      stocks_and_info = self.__get_stocks_list_from_screeners()

      # Get financial info for missing stocks (individually using Ticker)
      self.__get_missing_stocks_info(stocks_and_info)

      # Get the regularMarketPrice for each stock; keep a sum too
      closing_prices, closing_prices_sum = self.__get_last_closing_prices_and_sum(stocks_and_info)
      
      # Compare closing_prices_sum to sum of last row in the historical data
      if self.__check_closing_prices_are_equal(closing_prices_sum):
        self.pai("Skipping update of historical data.")
        self.__send_last_closing_price_update_email()
        return
      
      self.__add_last_closing_price_to_historical_data(closing_prices)

      # Update the historical data in S3
      self.__upload_new_historical_data_to_s3(self.__hdata)
      self.__send_last_closing_price_update_email()

    except Exception as e:
      raise e

  #endregion

  #region PreMarket
  def __sort_and_enlist_processed_symbols(self, symbols: dict) -> None:
    # Percentage changes for different times' periods
    one_day = dict(sorted(symbols['1-day'].items(), key=lambda item: item[1]))
    five_day = dict(sorted(symbols['5-day'].items(), key=lambda item: item[1]))
    one_month = dict(sorted(symbols['1-month'].items(), key=lambda item: item[1]))
    six_month = dict(sorted(symbols['6-month'].items(), key=lambda item: item[1]))
    one_year = dict(sorted(symbols['1-year'].items(), key=lambda item: item[1]))
    # Surface Area Ratio
    one_year_sa = dict(sorted(symbols['surface-area-ratio'].items(), key=lambda item: item[1], reverse=True))

    # Convert dictionaries to lists for easier indexing
    _1d_items = list(one_day.items())
    _5d_items = list(five_day.items())
    _1m_items = list(one_month.items())
    _6m_items = list(six_month.items())
    _1y_items = list(one_year.items())
    _1y_sa_items = list(one_year_sa.items())

    return _1d_items, _5d_items, _1m_items, _6m_items, _1y_items, _1y_sa_items

  def __dataframize_processed_data(self, symbols: dict) -> None:
    """Given a dictionary of processed symbols with their percentage changes and surface area ratios,
    create a dataframe with the data organized in columns for each metric."""
    _1d, _5d, _1m, _6m, _1y, _1y_sa = self.__sort_and_enlist_processed_symbols(symbols)
    
    # Create a dataframe of length max_length with columns '1dVal', '1dSym', '5dVal', '5dSym', '1mVal', '1mSym', '6mVal', '6mSym', '1yVal', '1ySym', '1ySaVal', '1ySaSym'
    max_length = max(len(_1d), len(_5d), len(_1m), len(_6m), len(_1y), len(_1y_sa))
    df = pd.DataFrame(index=range(max_length), columns=[
        '1dVal', '1dSym', '5dVal', '5dSym', '1mVal', '1mSym', '6mVal', '6mSym', '1yVal', '1ySym', '1ySaVal', '1ySaSym'
    ])

    for i in range(max_length):
        # 1-day data
        df.at[i, '1dVal'] = _1d[i][1] if i < len(_1d) else None
        df.at[i, '1dSym'] = _1d[i][0] if i < len(_1d) else None

        # 5-day data
        df.at[i, '5dVal'] = _5d[i][1] if i < len(_5d) else None
        df.at[i, '5dSym'] = _5d[i][0] if i < len(_5d) else None

        # 1-month data
        df.at[i, '1mVal'] = _1m[i][1] if i < len(_1m) else None
        df.at[i, '1mSym'] = _1m[i][0] if i < len(_1m) else None

        # 6-month data
        df.at[i, '6mVal'] = _6m[i][1] if i < len(_6m) else None
        df.at[i, '6mSym'] = _6m[i][0] if i < len(_6m) else None

        # 1-year data
        df.at[i, '1yVal'] = _1y[i][1] if i < len(_1y) else None
        df.at[i, '1ySym'] = _1y[i][0] if i < len(_1y) else None

        # Surface area data
        df.at[i, '1ySaVal'] = _1y_sa[i][1] if i < len(_1y_sa) else None
        df.at[i, '1ySaSym'] = _1y_sa[i][0] if i < len(_1y_sa) else None

    return df

  def __log_processed_symbols(self, group_name: str, _data: pd.DataFrame) -> None:
    """Log processed symbols to a specified Excel sheet."""
    # Save to Excel
    excel_local_file_path = self.cfg['excel_temp_file_path']
    
    # Check if file exists to determine mode
    # TODO if the file exists but is old, or unrelated, we should delete it and create a new one
    if os.path.exists(excel_local_file_path):
        # Append mode - add new sheet to existing file
        with pd.ExcelWriter(excel_local_file_path, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
            _data.to_excel(writer, sheet_name=group_name, index=False)
    else:
        # Create new file
        _data.to_excel(excel_local_file_path, sheet_name=group_name, index=False)

  def __group_stocks_by_market_cap(self, stocks_and_info: dict) -> dict:
    # Almost following megacap, largecap and midcap grouping (with the exception that largecap is above 20B instead of 10B)
    grouped = { 'above200B': {}, 'above20B': {}, 'above2B': {}, }
    no_market_cap_symbols = []
    cap_too_low_symbols = []

    for ticker, info in stocks_and_info.items():
      if 'marketCap' not in info:
        no_market_cap_symbols.append(ticker)
        continue

      market_cap = info.get('marketCap', 0)
      if market_cap >= 200_000_000_000:
        grouped['above200B'][ticker] = info
      elif market_cap >= 20_000_000_000:
        grouped['above20B'][ticker] = info
      elif market_cap >= 2_000_000_000:
        grouped['above2B'][ticker] = info
      else:
        # We are placing them into "above2B" because chances are that when originally processed by historical data generation, they were above the threshold
        grouped['above2B'][ticker] = info
        cap_too_low_symbols.append(ticker)
    
    self.pai(f"Grouped stocks by market cap: { {k: len(v) for k, v in grouped.items()} }")

    if no_market_cap_symbols:
      self.paw(f"Could not find marketCap for {len(no_market_cap_symbols)} symbols: {no_market_cap_symbols}. They were skipped from grouping.")

    # Add warning for awareness
    if cap_too_low_symbols:
      self.paw(f"Found {len(cap_too_low_symbols)} symbols with market cap too low for grouping: {cap_too_low_symbols}. They were placed in `above2B`.")

    return grouped

  def __process_pre_market_data(self, grouped_stocks_and_info: dict) -> dict:
    stocks_in_hd = self.__hdata.columns[self.__hdata.columns != 'date'].tolist()
    key_error_failed_symbols, key_err_msg = [], None
    processed = {
        '1-day': {},
        '5-day': {},
        '1-month': {},
        '6-month': {},
        '1-year': {},
        'surface-area-ratio': {},
    }

    for ticker in stocks_in_hd:
      try:
        # If the ticker is not in the current group, skip it
        if ticker not in grouped_stocks_and_info:
           continue

        price_data = grouped_stocks_and_info[ticker]
        # Get the column with the name of 'ticker' in the historical data dataframe
        h_data = self.__hdata[ticker].copy()
        curr_price = price_data['preMarketPrice']

        one_day_max = h_data[-1:].max()  # Last day, since the last day is the previous close
        single_day_percent_change = round(((curr_price - one_day_max) / one_day_max) * 100, 2)

        five_day_max = h_data[-5:].max()
        five_day_percent_change = round(((curr_price - five_day_max) / five_day_max) * 100, 2)

        # On average, there are 20-23 business days in a month, picking 22 as my lucky number
        one_month_max = h_data[-22:].max()
        one_month_percent_change = round(((curr_price - one_month_max) / one_month_max) * 100, 2)

        six_month_max = h_data[-130:].max()
        six_month_percent_change = round(((curr_price - six_month_max) / six_month_max) * 100, 2)

        one_year_max = h_data.max()
        one_year_percent_change = round(((curr_price - one_year_max) / one_year_max) * 100, 2)

        processed['1-day'][ticker] = single_day_percent_change
        processed['5-day'][ticker] = five_day_percent_change
        processed['1-month'][ticker] = one_month_percent_change
        processed['6-month'][ticker] = six_month_percent_change
        processed['1-year'][ticker] = one_year_percent_change

        above_over_below_ratio = self.__get_surface_area_ratio(h_data, curr_price)
        processed['surface-area-ratio'][ticker] = above_over_below_ratio
      except KeyError as KE:
        # If we are here, chances are high that the stock is not after-hours "tradeable" (we'll log it later)
        key_error_failed_symbols.append(ticker)
        key_err_msg = f"Details: {repr(KE)}."
      except Exception as E:
        self.paw(f"Error for symbol '{ticker}'. Details: {repr(E)}.")

    if key_error_failed_symbols:
      # It is a long list so I will not print out the symbols.
      # msg = f"Could not process ({len(key_error_failed_symbols)}) symbols (probably not pre-market tradeable): {key_error_failed_symbols}."
      msg = f"Could not process ({len(key_error_failed_symbols)}) symbols (probably not pre-market tradeable)."
      if key_err_msg:
        msg += f" {key_err_msg}"
      self.paw(msg)
    
    return processed

  def __send_pre_market_analysis_email(self) -> None:
    body = f"The pre-market prices were successfully processed and documented in the attached Excel file.\n\n"
    
    if self.warnings:
      body += "\nWarnings:\n"
      for warning in self.warnings:
        body += f"- {warning}\n"

    if self.infos:
      body += "\nInfos:\n"
      for info in self.infos:
        body += f"- {info}\n"

    self.emailer.set_email_params(
      to=self.cfg['email_to'], 
      subject="Pre-Market Prices Processed", 
      body=body
    )

    excel_local_file_path = self.cfg['excel_temp_file_path']
    if os.path.exists(excel_local_file_path):
      self.emailer.set_attachment(excel_local_file_path, self.cfg['excel_email_file_name'])

    self.emailer.send()

  def analyze_pre_market_prices(self) -> None:
    try:
      # Run checks
      # TODO add checks here
      # Check you are in pre-market hours (between 4:00 AM and 9:30 AM Pacific Time)

      # Download historical data
      self.__get_hist_data_from_s3(self.cfg['s3_historical_data_csv_name'])

      # Get stocks, and their financial data, from screeners
      stocks_and_info = self.__get_stocks_list_from_screeners()

      # Get financial info for missing stocks (individually using Ticker)
      self.__get_missing_stocks_info(stocks_and_info)

      # Group stocks by market cap
      grouped_stocks = self.__group_stocks_by_market_cap(stocks_and_info)

      # Process each group separately
      for group_name, group_stocks in grouped_stocks.items():
        print(f"Processing pre-market data for group '{group_name}' with {len(group_stocks)} stocks.")
        data_as_dicts = self.__process_pre_market_data(group_stocks)
        data_as_df = self.__dataframize_processed_data(data_as_dicts)
        self.__log_processed_symbols(group_name, data_as_df)

      self.__send_pre_market_analysis_email()

    except Exception as e:
      raise e
    
  #endregion