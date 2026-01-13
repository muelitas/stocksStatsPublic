# stocksStatsPublic
Fetch and maintain stocks historical data to provide stats via email

## Overview
This project is designed to fetch and maintain historical stock data for a list of specified stocks. It retrieves data from Yahoo Finance, stores it in S3, and sends periodic email updates with stock statistics. The application runs as an AWS Lambda function with automated CI/CD deployment through AWS CodeBuild. Scheduled executions are managed by AWS EventBridge for automated daily and monthly data processing tasks.

## Architecture

The project follows a modular architecture with clear separation of concerns:

- **Data Providers**: Multiple Yahoo Finance data sources (yfinance and yahooquery APIs)
- **Storage**: AWS S3 for persistent data storage
- **Communication**: Email notifications for alerts and reports
- **Deployment**: Docker containerized AWS Lambda function
- **Scheduling**: AWS EventBridge for automated execution of daily and monthly tasks
- **Automation**: Scenario-based execution for different types of analysis

## Features

### Stock Data Management
- Automated fetching of stock lists using Yahoo Finance screeners
- Historical data collection and maintenance
- Real-time price updates (pre-market and closing prices)
- Support for multiple stock exchanges (NASDAQ, NYSE, OTC)
- Portfolio tracking for currently invested stocks

### Data Processing Scenarios
1. **Stock List Management** (`upsert_stocks_list`)
   - Updates comprehensive stock lists from market screeners
   - Filters stocks by market cap threshold ($5B+)
   - Handles up to 250 symbols per screener
   - Scheduled for first Saturday of every month

2. **Historical Data Updates** (`upsert_historical_data`)
   - Bulk updates of historical price data
   - Data validation and integrity checks
   - Scheduled for first Sunday of every month

3. **Daily Price Updates** (`update_last_closing_price`)
   - Updates closing prices for all tracked stocks
   - Runs weekdays at 6:30 PM ET

4. **Pre-market Analysis** (`analyze_pre_market_prices`)
   - Analyzes pre-market price movements
   - Generates alerts for significant changes
   - Runs weekdays at 6:24 AM ET

5. **Mid-day Processing** (`regular_market_processing`)
   - Intra-day market analysis (planned feature)

### Email Reporting
- Automated email alerts for errors and warnings
- Excel-based analysis reports
- Customizable notification recipients

## Technical Stack

- **Runtime**: Python 3.13
- **Cloud Platform**: AWS Lambda, S3, ECR, EventBridge
- **Data Sources**: Yahoo Finance (yfinance, yahooquery)
- **Data Processing**: pandas, openpyxl
- **Communication**: Email via SMTP
- **Reporting**: reportlab (PDF generation)
- **Infrastructure**: Docker, AWS CodeBuild

## Configuration

The application uses environment variables for configuration:

### Required Environment Variables
```bash
GMAIL_APP_USER=your-email@gmail.com      # Gmail app user for sending emails
GMAIL_APP_PSW=your-app-password          # Gmail app password
EMAIL_TO=recipient@email.com             # Email recipient for notifications
```

### S3 Configuration
- **Bucket**: `stocks-stats`
- **Files**:
  - `all_stocks.csv` - Complete stock list with metadata
  - `otc_stocks.txt` - OTC stock symbols
  - `currently_invested_stocks.txt` - Portfolio stock symbols
  - `stocks_historical_data.csv` - Historical price data
  - `ms_screeners.txt` - Market screener configuration

### Thresholds
- Market cap threshold: $5,000,000,000
- Symbols per screener: 250
- Rate limiting: 20 stocks per batch for API calls

## Deployment

### AWS Lambda Deployment
The project is containerized and deployed as an AWS Lambda function:

1. **Build**: Docker image built using Python 3.13 AWS Lambda base image
2. **Registry**: Pushed to AWS ECR
3. **Deploy**: Lambda function updated via AWS CodeBuild
4. **Scheduling**: AWS EventBridge schedules trigger Lambda function with specific scenarios
5. **Trigger**: Event-driven execution with scenario parameters

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GMAIL_APP_USER="your-email@gmail.com"
export GMAIL_APP_PSW="your-app-password"
export EMAIL_TO="recipient@email.com"

# Run locally
python src/main.py
```

## Usage

### Lambda Event Format
```json
{
  "scenario": "update_last_closing_price"
}
```

### Available Scenarios
- `upsert_stocks_list` - Update stock lists from screeners
- `upsert_historical_data` - Bulk historical data updates  
- `update_last_closing_price` - Daily closing price updates
- `analyze_pre_market_prices` - Pre-market analysis
- `regular_market_processing` - Mid-day processing (planned)

## Dependencies

See [requirements.txt](requirements.txt) for the complete list:
- `reportlab` - PDF generation
- `yahooquery` - Yahoo Finance API client
- `yfinance` - Alternative Yahoo Finance API
- `boto3` - AWS SDK
- `openpyxl` - Excel file handling

## Error Handling

The application includes comprehensive error handling:
- Email notifications for all exceptions
- Detailed logging and warning systems
- Data validation and integrity checks
- Graceful handling of API rate limits

## Future Enhancements

- [ ] Selenium webdriver for enhanced data scraping
- [ ] Direct Fidelity integration
- [ ] Enhanced OOP refactoring of StocksManager
- [ ] Improved stock data provider management
- [ ] Regular market processing implementation

