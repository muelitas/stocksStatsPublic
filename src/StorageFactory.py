from StorageProviders import StorageProviders as Providers
from StorageProviderInterface import StorageProviderInterface as iProvider
from StorageAwsS3Provider import StorageAwsS3Provider

class StorageFactory:
  """Factory class to create storage providers"""
  
  @staticmethod
  def create_provider(provider: Providers) -> iProvider:
    if provider == Providers.AWS_S3:
      return StorageAwsS3Provider()
    else:
      raise ValueError(f"Unsupported provider type: {provider}")