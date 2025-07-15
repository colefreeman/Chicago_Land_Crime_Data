import os
import pandas as pd


@data_loader
def load_raw_data(*args, **kwargs):
    """
    Load raw data from a CSV file or other source.
    Replace the file path with your actual data source.
    """

    # Define the path to the raw data file
    data_path = os.getenv("RAW_DATA_PATH", "path/to/your/data.csv")

    # Read the data into a pandas DataFrame
    raw_data = pd.read_csv(data_path)

    # Return the loaded data
    return raw_data
