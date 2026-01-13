import io

import boto3
import botocore
import pandas as pd

from StorageProviderInterface import StorageProviderInterface as ProviderInterface

class StorageAwsS3Provider(ProviderInterface):
  def __init__(self):
    self.s3_client = boto3.client("s3")

  def check_existence(self, bucket_name: str, bucket_key: str) -> bool:
    try:
      self.s3_client.head_object(Bucket=bucket_name, Key=bucket_key)
      return True
    except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == "404":
        return False
      else:
        print(f"Unexpected error occurred while checking S3 object existence: {e}")
        raise e

  def create(self, bucket_name: str, bucket_key: str, data: pd.DataFrame | list) -> None:
    if not bucket_key or not bucket_name:
      raise ValueError("The 'bucket_key' and 'bucket_name' must be non-empty strings.")
    
    if bucket_key.endswith('.csv'):
      if not isinstance(data, pd.DataFrame):
        raise ValueError("Data must be a DataFrame to create a CSV file in S3.")
      
      self.__create_csv_file_in_s3(bucket_name, bucket_key, data)
    else:
      raise NotImplementedError(f"Creating files of type other than CSV is not implemented. Provided key (filename): {bucket_key}")

  def read(self, bucket_name: str, bucket_key: str) -> pd.DataFrame | list[str]:
    if not self.check_existence(bucket_name, bucket_key):
      raise FileNotFoundError(f"The object {bucket_key} does not exist in bucket {bucket_name}.")
    
    if not bucket_key or not bucket_name:
      raise ValueError("The 'bucket_key' and 'bucket_name' must be non-empty strings.")

    
    if bucket_key.endswith('.csv'):
      return self.__read_csv_file_from_s3(bucket_name, bucket_key)
    elif bucket_key.endswith('.txt'):
      return self.__read_txt_file_from_s3(bucket_name, bucket_key)
    else:
      raise NotImplementedError(f"Reading files of type other than CSV is not implemented. Provided key (filename): {bucket_key}")

  def update(self) -> None:
    raise NotImplementedError("Update operation is not implemented in AwsS3Provider.")

  def delete(self) -> None:
    raise NotImplementedError("Delete operation is not implemented in AwsS3Provider.")

  #region Private methods
  def __read_csv_file_from_s3(self, bucket: str, csv_file_name: str) -> pd.DataFrame:
    response = self.s3_client.get_object(Bucket=bucket, Key=csv_file_name)
    body = response['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(body))
    return df
  
  def __read_txt_file_from_s3(self, bucket, txt_file_name) -> list:
    response = self.s3_client.get_object(Bucket=bucket, Key=txt_file_name)
    body = response['Body'].read().decode('utf-8')
    text_lines = body.splitlines()

    # If the last line is empty, remove it
    if text_lines and text_lines[-1] == '':
      text_lines.pop()

    return text_lines

  def __create_csv_file_in_s3(self, bucket: str, csv_file_name: str, df: pd.DataFrame) -> None:
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    self.s3_client.put_object(Bucket=bucket, Key=csv_file_name, Body=csv_buffer.getvalue())
    csv_buffer.close()

  #endregion