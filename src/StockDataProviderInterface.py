from abc import ABC, abstractmethod
import pandas as pd

class StockDataProvider(ABC):
    """Abstract base class for stock data providers"""
    
    @abstractmethod
    def get_historical_data(self, symbols: list, period: str = None, start: str = None, end: str = None) -> pd.DataFrame:
        pass

    # TODO add format_historical_data method?
    
    @abstractmethod
    def get_current_prices(self, symbols: list) -> dict:
        pass
    
    @abstractmethod
    def get_stock_info(self, symbol: str) -> dict:
        pass

    @abstractmethod
    def get_stocks_info(self, symbols: list) -> dict:
        pass