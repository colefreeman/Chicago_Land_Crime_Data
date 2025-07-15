import pandas as pd


@transformer
def clean_data(data: pd.DataFrame, *args, **kwargs):
    """
    Perform data cleaning operations such as handling missing values,
    removing duplicates, and correcting data types to ensure data quality.
    """

    # Drop duplicate rows to ensure data uniqueness
    data = data.drop_duplicates()

    # Handle missing values
    # For example, fill missing numerical columns with median
    for column in data.select_dtypes(include=["number"]).columns:
        median_value = data[column].median()
        data[column] = data[column].fillna(median_value)

    # Fill missing categorical columns with mode
    for column in data.select_dtypes(include=["object", "category"]).columns:
        mode_value = data[column].mode()
        if not mode_value.empty:
            data[column] = data[column].fillna(mode_value[0])

    # Correct data types if necessary
    # Example: convert date columns to datetime
    date_columns = [col for col in data.columns if "date" in col.lower()]
    for date_col in date_columns:
        data[date_col] = pd.to_datetime(data[date_col], errors="coerce")

    # Additional data quality checks can be added here

    return data
