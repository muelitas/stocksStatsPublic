from StorageProviders import StorageProviders as Providers
import pandas as pd
from StorageFactory import StorageFactory

class StorageProviderManager:
  """Main class that uses the factory and provides a unified interface"""
  
  def __init__(self, provider: Providers):
    self.provider = StorageFactory.create_provider(provider)

  def check_existence(
      self,
      bucket_name: str = None, 
      bucket_key: str = None,
      **kwargs
    ) -> bool:
    try:
      return self.provider.check_existence(
        bucket_name=bucket_name,
        bucket_key=bucket_key,
        **kwargs
      )
    except Exception as e:
      raise e
  
  def create(
      self, 
      bucket_name: str = None, 
      bucket_key: str = None, 
      data: pd.DataFrame | list = None, 
      **kwargs
    ) -> None:
    try:
      return self.provider.create(
        bucket_name=bucket_name,
        bucket_key=bucket_key,
        data=data,
        **kwargs
      )
    except Exception as e:
      raise e

  def read(
      self, 
      bucket_name: str = None, 
      bucket_key: str = None, 
      **kwargs
    ) -> pd.DataFrame | list[str]:
    try:
      return self.provider.read(
        bucket_name=bucket_name,
        bucket_key=bucket_key,
        **kwargs
      )
    except Exception as e:
      raise e
    
  def update(self, **kwargs) -> None:
    try:
      return self.provider.update(**kwargs)
    except Exception as e:
      raise e

  def delete(self, **kwargs) -> None:
    try:
      return self.provider.delete(**kwargs)
    except Exception as e:
      raise e