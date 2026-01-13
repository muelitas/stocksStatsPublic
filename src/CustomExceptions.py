
class NanValuesInHistoricalData(Exception):
  """A custom exception for when there are NaN values in the historical data that could not be fixed."""
  def __init__(self, message):
    self.message = message
    super().__init__(self.message)

class DiffDateRangesBetweenDataframes(Exception):
  """A custom exception for when there are different date ranges between dataframes."""
  def __init__(self, message):
    self.message = message
    super().__init__(self.message)