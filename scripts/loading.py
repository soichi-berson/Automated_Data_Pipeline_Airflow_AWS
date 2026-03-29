import pandas as pd
import logging


def loading(file_path: str) -> pd.DataFrame:
    """
    Load dataset and perform basic validation.
    """

    logging.info(f"Loading dataset from: {file_path}")

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        logging.error(f"Failed to load file: {e}")
        raise

    logging.info(f"Dataset loaded successfully. Shape: {df.shape}")

    # Validate required column
    if "delivery_date" not in df.columns:
        logging.error("Missing required column: delivery_date")
        raise ValueError("Missing required column: delivery_date")

    # Convert date
    df["delivery_date"] = pd.to_datetime(
        df["delivery_date"],
        errors="coerce"
    )

    invalid_dates = df["delivery_date"].isna().sum()

    if invalid_dates > 0:
        logging.warning(f"{invalid_dates} delivery_date values could not be parsed.")

    logging.info("Loading step completed successfully.")

    return df