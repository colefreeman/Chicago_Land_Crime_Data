from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
import numpy as np
import pandas as pd


@transformer
def transform_features(data: pd.DataFrame, *args, **kwargs):
    """
    Transform the cleaned data by creating new features, encoding categorical variables,
    and normalizing numerical data to prepare for modeling.
    """

    # Make a copy of the input data to avoid mutating the original DataFrame
    df = data.copy()

    # Example feature engineering: create a new feature based on existing columns
    # Assuming 'latitude' and 'longitude' columns exist
    if "latitude" in df.columns and "longitude" in df.columns:
        df["lat_lon_sum"] = df["latitude"] + df["longitude"]
        df["lat_lon_diff"] = df["latitude"] - df["longitude"]

    # Encode categorical variables using OneHotEncoder
    categorical_cols = df.select_dtypes(include=["object", "category"]).columns.tolist()

    if categorical_cols:
        encoder = OneHotEncoder(sparse=False, handle_unknown="ignore")
        encoded_cats = encoder.fit_transform(df[categorical_cols])
        encoded_cat_df = pd.DataFrame(
            encoded_cats,
            columns=encoder.get_feature_names_out(categorical_cols),
            index=df.index,
        )
        # Concatenate encoded features and drop original categorical columns
        df = pd.concat([df, encoded_cat_df], axis=1)
        df.drop(columns=categorical_cols, inplace=True)

    # Normalize numerical features using StandardScaler
    numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if numerical_cols:
        scaler = StandardScaler()
        df[numerical_cols] = scaler.fit_transform(df[numerical_cols])

    # Return the transformed DataFrame
    return df
