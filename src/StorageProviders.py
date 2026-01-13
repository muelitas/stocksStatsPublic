from enum import Enum

class StorageProviders(Enum):
  AWS_S3 = "aws_s3"
  REDSHIFT = "redshift" # Not implemented but adding it as placeholder
  POSTGRESQL = "postgresql" # Not implemented but adding it as placeholder
  # GCP, Azure, IBM, etc.