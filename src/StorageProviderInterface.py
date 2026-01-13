from abc import ABC, abstractmethod
import pandas as pd

class StorageProviderInterface(ABC):
    """Abstract base class for storage providers"""

    # @abstractmethod
    # def __init__(self):
    #     pass

    @abstractmethod
    def check_existence(self, bucket_name: str, bucket_key: str) -> bool:
        pass

    @abstractmethod
    def create(self, bucket_name: str, bucket_key: str, data: pd.DataFrame | list) -> None:
        pass
    
    @abstractmethod
    def read(self, bucket_name: str, bucket_key: str) -> pd.DataFrame | list[str]:
        pass
    
    @abstractmethod
    def update(self) -> None:
        pass

    @abstractmethod
    def delete(self) -> None:
        pass