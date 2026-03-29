import pandas as pd
import logging

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean dataset:
    - Parse delivery_date
    - Convert numeric columns
    - Calculate gross_sales safely
    """

    df = df.copy()

    # Convert delivery_date
    df["delivery_date"] = pd.to_datetime(
        df["delivery_date"],
        errors="coerce"
    )

    # Check for invalid dates
    if df["delivery_date"].isna().any():
        logging.warning("Some delivery_date values could not be parsed.")

    numeric_cols = [
        "total_sales",
        "shipping_cost",
        "discount",
        "quantity"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Safe gross_sales calculation
    df["gross_sales"] = (
        (df["total_sales"] - df["shipping_cost"]) /
        (1 - df["discount"]).replace(0, pd.NA)
    )

    return df