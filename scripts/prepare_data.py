
import pandas as pd
import logging
from datetime import timedelta

logger = logging.getLogger(__name__)

def prepare_reporting_data(
    df: pd.DataFrame,
    today: pd.Timestamp
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Create one-week and four-week rolling DataFrames
    and add week labels to one_month_df.
    """

    logger.info("Starting prepare_reporting_data step.")

    if "delivery_date" not in df.columns:
        logger.error("Missing required column: delivery_date")
        raise ValueError("Missing required column: delivery_date")

    df = df.copy()

    df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce")
    
    today = pd.Timestamp(today).normalize()

    one_week_before = today - timedelta(days=6)
    one_month_before = today - timedelta(days=27)

    logger.info(
        f"Filtering data between "
        f"{one_month_before.date()} and {today.date()}"
    )

    one_week_df = df.loc[
        df["delivery_date"].between(one_week_before, today)
    ].copy()

    one_month_df = df.loc[
        df["delivery_date"].between(one_month_before, today)
    ].copy()

    logger.info(
        f"one_week_df rows: {len(one_week_df)}, "
        f"one_month_df rows: {len(one_month_df)}"
    )

    if one_month_df.empty:
        logger.warning("one_month_df is empty after filtering.")

    # Add week label
    one_month_df["week"] = (
        "week-" +
        (
            ((today - one_month_df["delivery_date"]).dt.days // 7) + 1
        ).astype(str)
    )

    logger.info("Week labels successfully created.")

    logger.info("prepare_reporting_data step completed.")

    return one_week_df, one_month_df

