import os

C = {
  # Email configuration
   "email_user": os.environ['GMAIL_APP_USER'],
   "email_pwd": os.environ['GMAIL_APP_PSW'],
   "email_to": os.environ['EMAIL_TO'],

   # S3 configuration
   "s3_bucket": 'stocks-stats',
   "s3_all_stocks_csv_name": 'all_stocks.csv', # Sample available in src/s3_files_samples
   "s3_otc_stocks_txt_name": 'otc_stocks.txt', # Sample available in src/s3_files_samples
   "s3_currently_invested_stocks_txt_name": 'currently_invested_stocks.txt', # Sample available in src/s3_files_samples
   "s3_historical_data_csv_name": 'stocks_historical_data.csv', # Sample available in src/s3_files_samples
   "s3_screeners_file_name": 'ms_screeners.txt', # Sample available in src/s3_files_samples

  # Excel configuration
  "excel_temp_file_path": '/tmp/stocks_analysis.xlsx',
  "excel_email_file_name": 'stocks_analysis.xlsx',

  # Screeners configuration
  'symbols_per_screener': 250,
  'market_cap_threshold': 5_000_000_000,
}