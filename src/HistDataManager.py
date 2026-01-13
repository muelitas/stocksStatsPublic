from datetime import datetime, timezone
import sys
import time
import zoneinfo

import pandas as pd

from CustomExceptions import NanValuesInHistoricalData, DiffDateRangesBetweenDataframes
from Emailer import Emailer
from StockDataProviderManager import StockDataProviderManager
from StorageProviderManager import StorageProviderManager

class HistDataManager:
  def __init__(self, storage_manager: StorageProviderManager, emailer: Emailer, yahoo_finance_data_manager: StockDataProviderManager, yahoo_query_data_manager: StockDataProviderManager, cfg: dict):
    self.s3_mgr = storage_manager
    self.emailer = emailer
    self.yfinance_manager = yahoo_finance_data_manager
    self.yquery_manager = yahoo_query_data_manager
    # TODO once config is defined, validate it here and create class attributes
    self.cfg = cfg

    self.__hd_df = None
    self.infos = []
    self.warnings = []

    self.in_update_mode = None

  # Short for Print and Warn
  def paw(self, msg: str) -> None:
    print(msg)
    self.warnings.append(msg)

  # Short for Print and Info
  def pai(self, msg: str) -> None:
    print(msg)
    self.infos.append(msg)

  #region Checks
  def __validate_time_frame_for_upsert(self) -> None:
    # Get current time in New York timezone (accounts for DST automatically)
    ny_tz = zoneinfo.ZoneInfo("America/New_York")
    now_ny = datetime.now(ny_tz)
    
    # If we are on Saturday or Sunday, we are good to go, return early
    if now_ny.weekday() in [5, 6]:
      return

    # Otherwise, check for the time of the day.
    # This validation will not catch weekday holidays, we have a guard for that in the StocksManager when updating last closing prices. TODO find a way to guard here as well
    self.paw("Upserting historical data is being attempted on a weekday. Please ensure this is done during after-market hours (4:00 p.m. to 9:30 a.m. ET) and not on a Holiday.")

    # In Eastern Time, market is open from 9:30 AM to 4:00 PM
    # So we can only upsert OUTSIDE these hours (after 4:00 PM or before 9:30 AM)
    market_open_et = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close_et = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)
    
    # Check if current time is during market hours (when we should NOT upsert)
    if market_open_et <= now_ny <= market_close_et:
      raise ValueError(f"Upserting historical data can only be done during after-market hours (after 4:00 p.m. ET or before 9:30 a.m. ET). Current Eastern Time is {now_ny.strftime('%Y-%m-%d %H:%M:%S %Z')}.")

  def __check_for_list_uniqueness(self, lst: list, obj_key: str) -> None:
    if len(lst) != len(set(lst)):
      warning_msg = f"Warning: The list in {obj_key} contains duplicate entries."
      print(warning_msg)
      self.warnings.append(warning_msg)
  
  def __check_for_columns_with_multiple_nans(self, df: pd.DataFrame) -> pd.DataFrame:
    # If a column has more than 1 Nan value, remove the symbol from the dataframe and log a warning
    cols_with_multiple_nans = df.columns[df.isna().sum() > 1].tolist()
    if cols_with_multiple_nans:
      self.paw(f"\t\tColumns with more than 1 NaN value found: {cols_with_multiple_nans}. Removing them from the historical data.")

      for symbol in cols_with_multiple_nans:
        df.drop(columns=[symbol], inplace=True)

    return df

  def __check_for_non_last_row_nans(self, df: pd.DataFrame) -> pd.DataFrame:
    # Check if there are NaN values in rows other than the last row
    non_last_rows = df.iloc[:-1]
    cols_with_nans_in_non_last_rows = non_last_rows.columns[non_last_rows.isna().any()].tolist()
    if cols_with_nans_in_non_last_rows:
      self.paw(f"\t\tColumns with NaN values in non-last rows found: {cols_with_nans_in_non_last_rows}. Removing them from the historical data.")

      for symbol in cols_with_nans_in_non_last_rows:
        df.drop(columns=[symbol], inplace=True)

    return df

  def __check_for_last_row_nans(self, df: pd.DataFrame) -> list:
    last_row = df.iloc[-1]
    cols_with_nan_in_last_row = last_row[last_row.isna()].index.tolist()
    if cols_with_nan_in_last_row:
      self.paw(f"\t\tColumns with NaN values in last row found: {cols_with_nan_in_last_row}.")

    return cols_with_nan_in_last_row

  def __attempt_fix_last_row_nans(self, cols_with_nan_in_last_row: list, stock_data_mgr: StockDataProviderManager, df: pd.DataFrame) -> pd.DataFrame:
    # For each column with NaN in the last row, use tickers' info to look at their data
    tickers_info = stock_data_mgr.get_stocks_info(cols_with_nan_in_last_row)
    for symbol in cols_with_nan_in_last_row:
      try:
        price_data = tickers_info[symbol]
        if 'regularMarketPrice' in price_data and price_data['regularMarketPrice'] is not None:
          df.at[df.index[-1], symbol] = price_data['regularMarketPrice']
          print(f"\tSuccessfully fixed NaN for symbol {symbol} in the last row using regularMarketPrice: {price_data['regularMarketPrice']}")
        else:
          # Remove the symbol from the dataframe since we couldn't get its price
          self.paw(f"\tFailed to fix NaN for symbol {symbol} in the last row: no valid regularMarketPrice found in price data: {price_data}. Removing it from historical data.")
          df.drop(columns=[symbol], inplace=True)

      except Exception as e:
        # Remove the symbol from the dataframe since something went wrong
        self.paw(f"\tException occurred while trying to fix NaN for symbol {symbol} in the last row: {repr(e)}. Removing it from historical data.")
        print(str(e))
        df.drop(columns=[symbol], inplace=True)

    return df

  def __check_for_nans_in_df(self, df: pd.DataFrame) -> None:
    if df.isna().any().any():
      cols_with_nans = df.columns[df.isna().any()].tolist()
      self.paw(f"\tThe dataframe still contains NaN values after attempting to fix them. Columns with NaNs: {cols_with_nans}.")
      raise NanValuesInHistoricalData(f"The dataframe still contains NaN values after attempting to fix them. Columns with NaNs: {cols_with_nans}")
    
  #endregion

  #region Others
  def __fetch_hist_data(self, stock_data_mgr: StockDataProviderManager, batch: list) -> pd.DataFrame:
    df = None
    if self.in_update_mode:
      start_date, end_date = self.__hd_df['date'].min(), self.__hd_df['date'].max()
      # Add a buffer of 2 days on each side to account for data providers cutoffs; only doing this on update mode as I have an inner join later
      start_date = (pd.to_datetime(start_date) - pd.Timedelta(days=2)).strftime('%Y-%m-%d')
      end_date = (pd.to_datetime(end_date) + pd.Timedelta(days=2)).strftime('%Y-%m-%d')
      df = stock_data_mgr.get_historical_data(batch, start=start_date, end=end_date)
    else:
      df = stock_data_mgr.get_historical_data(batch, period='1y') # '1mo' for 1 month

    df = self.__check_for_columns_with_multiple_nans(df)
    df = self.__check_for_non_last_row_nans(df)
    last_row_nans_stocks = self.__check_for_last_row_nans(df)
    df = self.__attempt_fix_last_row_nans(last_row_nans_stocks, stock_data_mgr, df)

    # Since the three methods above may have removed some columns, check if the dataframe is empty
    if df.empty:
      self.paw("The historical data dataframe is empty after removing columns with multiple NaNs or NaNs in non-last rows. No data to process.")
      return df #early return

    self.__check_for_nans_in_df(df)
    return df
  
  def __aggregate_hd_dataframes(self, base_df: pd.DataFrame, new_df: pd.DataFrame) -> None:
    agg_df = pd.merge(base_df, new_df, on='date', how='inner')
    if self.in_update_mode:
      orig_start_date, orig_end_date = self.__hd_df['date'].min(), self.__hd_df['date'].max()
    else:
      orig_start_date, orig_end_date = base_df['date'].min(), base_df['date'].max()

    agg_start_date, agg_end_date = agg_df['date'].min(), agg_df['date'].max()

    if orig_start_date != agg_start_date or orig_end_date != agg_end_date:
      raise DiffDateRangesBetweenDataframes(f"The aggregated dataframe has a different date range ({agg_start_date} to {agg_end_date}) than the original historical data ({orig_start_date} to {orig_end_date}).")
    
    return agg_df
  
  def __wait_if_less_than_x_seconds(self, start_time: float, wait_seconds: int = 62) -> None:
    elapsed_time = time.time() - start_time
    if elapsed_time < wait_seconds:
      print(f"Waiting {wait_seconds - elapsed_time} seconds to avoid Yahoo Query API rate limits")
      time.sleep(wait_seconds - elapsed_time)

  #endregion

  #region Update
  def __get_current_historical_data(self) -> pd.DataFrame:
    hd_df = self.s3_mgr.read(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_historical_data_csv_name'])
    self.pai(f"\tSuccessfully read historical data from {self.cfg['s3_historical_data_csv_name']} in bucket {self.cfg['s3_bucket']}")
    self.pai(f"\tIt has {hd_df.shape[0]} rows and {hd_df.shape[1]} columns")
    start_date, end_date = hd_df['date'].min(), hd_df['date'].max()
    self.pai(f"\tIt ranges from {start_date} to {end_date}")
    return hd_df
  
  def __get_stocks_to_update(self) -> list[str]:
    ''' Get the list of stocks that are in the stocks list but not in the historical data dataframe '''
    stocks_list_ground_truth = self.s3_mgr.read(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_all_stocks_csv_name'])['symbol'].tolist()
    self.__check_for_list_uniqueness(stocks_list_ground_truth, self.cfg['s3_all_stocks_csv_name'])
    self.pai(f"\tGathered {len(stocks_list_ground_truth)} stocks from {self.cfg['s3_all_stocks_csv_name']}")

    # For all the stocks in the stocks list, check if they are in the historical data dataframe
    missing_stocks = [stock for stock in stocks_list_ground_truth if stock not in self.__hd_df.columns]
    self.pai(f"\tFound {len(missing_stocks)} missing stocks in historical data")

    return missing_stocks
  
  def __send_successful_update_email(self) -> None:
    body = f"The historical data was successfully updated.\n\n"
    if self.infos:
      body += "Infos:\n"
      for info in self.infos:
        body += f"- {info}\n"

    if self.warnings:
      body += "\n\nWarnings:\n"
      for warning in self.warnings:
        body += f"- {warning}\n"

    self.emailer.set_email_params(
      to=self.cfg['email_to'], 
      subject="Historical Data Updated", 
      body=body
    )
    self.emailer.send()

  def __fetch_hist_data_on_update(self, stocks_batches: list) -> pd.DataFrame:
    if not stocks_batches or len(stocks_batches) == 0:
      raise ValueError("No stock batches provided to fetch historical data.")
    
    agg_df = self.__hd_df.copy()
    self.pai(f"\t\tAt first, agg_df has {agg_df.shape[0]} rows and {agg_df.shape[1]} columns")
    start_time = time.time()
    alternate = 1  # 1 for yfinance, -1 for yahooquery

    for batch_index, batch in enumerate(stocks_batches):
      if (batch_index + 1) % 6 == 0:
        # To avoid hitting API rate limits, we will make sure we don't do more than 6 requests per minute
        self.__wait_if_less_than_x_seconds(start_time, wait_seconds=62)
        start_time = time.time()

      try:
        stock_data_mgr = self.yfinance_manager if alternate == 1 else self.yquery_manager
        df = self.__fetch_hist_data(stock_data_mgr, batch)
        alternate *= -1  # Switch between 1 and -1

        if df.empty:
          self.pai(f"\tNo historical data returned for batch {batch_index + 1}/{len(stocks_batches)}. Skipping this batch.")
        else:
          # For debugging purposes
          # print(df.head())
          # Aggregate the new data with the existing historical data
          agg_df = self.__aggregate_hd_dataframes(agg_df, df)
          print(f"\t\tAfter processing batch {batch_index + 1}/{len(stocks_batches)}, agg_df has {agg_df.shape[0]} rows and {agg_df.shape[1]} columns")
      except NanValuesInHistoricalData as E:
        msg = f"Error: {repr(E)}. Batch {batch_index + 1}/{len(stocks_batches)} (starting with stock {batch[0]}) will be skipped."
        self.paw(msg)
      except DiffDateRangesBetweenDataframes as E:
        msg = f"Error: {repr(E)}. Batch {batch_index + 1}/{len(stocks_batches)} (starting with stock {batch[0]}) will be skipped."
        self.paw(msg)
      except Exception as E:
        msg = f"An unexpected error occurred while trying to get historical data for the batch that started with symbol {batch[0]} and ended with symbol {batch[-1]}. Error details: {repr(E)}. We are skipping this batch."
        self.paw(msg)

    return agg_df

  def __update(self) -> None:
    """
    Compare the list of stocks in the stocks list with the columns in the historical data dataframe.
    For any stocks that are missing in the historical data, fetch their historical data and append it to the existing dataframe.
    """
    self.__hd_df = self.__get_current_historical_data()
    stocks_to_process = self.__get_stocks_to_update()
    # TODO keep track of whether or not the historical data was changed; if not, do not send email and do not upload to S3
    if len(stocks_to_process) == 0:
      self.pai("\tNo missing stocks found. Historical data is up to date.")
      return
    
    batch_size = 20
    batches = [stocks_to_process[i:i + batch_size] for i in range(0, len(stocks_to_process), batch_size)]

    self.__hd_df = self.__fetch_hist_data_on_update(batches)
    self.s3_mgr.create(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_historical_data_csv_name'], data=self.__hd_df)
    self.pai(f"\tSuccessfully updated historical data to {self.cfg['s3_historical_data_csv_name']} in bucket {self.cfg['s3_bucket']}")
    self.pai(f"\tIt now has {self.__hd_df.shape[0]} rows and {self.__hd_df.shape[1]} columns")
    self.__send_successful_update_email()

  #endregion

  #region Create
  def __fetch_hist_data_on_create(self, stocks_batches: list) -> pd.DataFrame:
    if not stocks_batches or len(stocks_batches) == 0:
      raise ValueError("No stock batches provided to fetch historical data.")

    dataframes = []
    start_time = time.time()
    alternate = 1  # 1 for yfinance, -1 for yahooquery

    # TODO create a class that handles this alternation setup (created issue: https://github.com/muelitas/stocksStats/issues/6)
    for batch_index, batch in enumerate(stocks_batches):
      if (batch_index + 1) % 6 == 0:
        # To avoid hitting API rate limits, we will make sure we don't do more than 6 requests per minute
        self.__wait_if_less_than_x_seconds(start_time, wait_seconds=62)
        start_time = time.time()

      try:
        stock_data_mgr = self.yfinance_manager if alternate == 1 else self.yquery_manager
        df = self.__fetch_hist_data(stock_data_mgr, batch)
        alternate *= -1  # Switch between 1 and -1

        if df.empty:
          self.pai(f"\tNo historical data returned for batch {batch_index + 1}/{len(stocks_batches)}. Skipping this batch.")
        else:
          # For debugging purposes
          # print(df.head())
          dataframes.append(df)
          print(f"\tSuccessfully fetched historical data for batch {batch_index + 1}/{len(stocks_batches)}.")
      except NanValuesInHistoricalData as E:
        msg = f"Error: {repr(E)}. The batch that started with symbol {batch[0]} and ended with symbol {batch[-1]} will be skipped."
        self.paw(msg)
      except Exception as E:
        msg = f"An unexpected error occurred while trying to get historical data for the batch that started with symbol {batch[0]} and ended with symbol {batch[-1]}. Error details: {repr(E)}. We are skipping this batch."
        self.paw(msg)

    if len(dataframes) == 0:
      raise ValueError("No historical data was fetched for any of the stock batches.")

    # Merge all dataframes on the 'date' column to combine all stocks with shared dates
    combined_df = None
    if len(dataframes) == 1:
      combined_df = dataframes[0]
    else:
      combined_df = dataframes[0]  # Start with the first dataframe
      for df in dataframes[1:]:
        combined_df = self.__aggregate_hd_dataframes(combined_df, df)

    return combined_df

  def __send_successful_create_email(self) -> None:
    body = f"The historical data was successfully created.\n\n"
    if self.infos:
      body += "Infos:\n"
      for info in self.infos:
        body += f"- {info}\n"

    if self.warnings:
      body += "\n\nWarnings:\n"
      for warning in self.warnings:
        body += f"- {warning}\n"

    self.emailer.set_email_params(
      to=self.cfg['email_to'], 
      subject="Historical Data Created", 
      body=body
    )
    self.emailer.send()

  def __create(self) -> None:
    stocks_to_process = self.s3_mgr.read(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_all_stocks_csv_name'])['symbol'].tolist()
    self.__check_for_list_uniqueness(stocks_to_process, self.cfg['s3_all_stocks_csv_name'])
    self.pai(f"\tGathered {len(stocks_to_process)} stocks from {self.cfg['s3_all_stocks_csv_name']}")

    batch_size = 20
    batches = [stocks_to_process[i:i + batch_size] for i in range(0, len(stocks_to_process), batch_size)]

    self.__hd_df = self.__fetch_hist_data_on_create(batches)
    self.s3_mgr.create(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_historical_data_csv_name'], data=self.__hd_df)
    self.pai(f"\tSuccessfully created historical data to {self.cfg['s3_historical_data_csv_name']} in bucket {self.cfg['s3_bucket']}")
    self.pai(f"\tIt now has {self.__hd_df.shape[0]} rows and {self.__hd_df.shape[1]} columns")
    self.__send_successful_create_email()

  #endregion

  #region Upsert
  def __send_failed_upsert_email(self, error: Exception, in_update_mode: bool | None) -> None:
    mode = "update" if in_update_mode else "create" if in_update_mode == False else "upsert"
    body = f"""
    An error occurred while trying to {mode} the historical data.

    Error details:
    {repr(error)}

    Infos:
    """
    for info in self.infos:
      body += f"- {info}\n"

    body += "\nWarnings:\n"
    for warning in self.warnings:
      body += f"- {warning}\n"

    self.emailer.set_email_params(
      to=self.cfg['email_to'], 
      subject=f"Failed to {mode} historical data", 
      body=body
    )

    self.emailer.send()

  def upsert(self):
    try:
      self.__validate_time_frame_for_upsert()
      all_stocks_file_exists = self.s3_mgr.check_existence(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_all_stocks_csv_name'])
      if not all_stocks_file_exists:
        raise ValueError(f"The stocks list file '{self.cfg['s3_all_stocks_csv_name']}' does not exist in bucket '{self.cfg['s3_bucket']}'. Cannot upsert historical data without the stocks csv file.")

      self.in_update_mode = self.s3_mgr.check_existence(bucket_name=self.cfg['s3_bucket'], bucket_key=self.cfg['s3_historical_data_csv_name'])
      # TODO after checking existence, check if the historical data file is empty or corrupted; if so, set in_update_mode to False

      if self.in_update_mode:
        self.pai("In Update mode")
        self.__update()
      else:
        self.pai("In Create mode")
        self.__create()
    except Exception as E:
      self.paw(f"An error occurred while upserting historical data: {repr(E)}")
      self.__send_failed_upsert_email(E, self.in_update_mode)
      # No need to raise the error since we will be sending an email

  #endregion

