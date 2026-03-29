import pandas as pd
import logging

logger = logging.getLogger(__name__)

def one_month_analysis(
    one_month_df: pd.DataFrame,
    today: pd.Timestamp
) -> dict:
    """
    Perform 4-week rolling analysis.

    Returns:
        dict containing:
            - weekly_summary (DataFrame)
            - weekly_orders (DataFrame)
            - wow_growth (float)
    """

    logger.info("Starting one_month_analysis step.")

    if one_month_df.empty:
        logger.warning("one_month_df is empty. Results may be incomplete.")

    required_cols = ["week", "total_sales", "quantity"]
    missing_cols = [col for col in required_cols if col not in one_month_df.columns]

    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = one_month_df.copy()

    # -----------------------
    # Weekly Revenue Summary
    # -----------------------
    weekly_summary = (
        df
        .groupby("week")
        .agg(
            total_revenue=("total_sales", "sum"),
            total_quantity=("quantity", "sum")
        )
        .reset_index()
    )

    logger.info(f"Weekly revenue summary created. Rows: {len(weekly_summary)}")

    # Extract numeric week
    weekly_summary["week_number"] = (
        weekly_summary["week"]
        .str.extract(r"(\d+)")
        .astype(int)
    )

    weekly_summary["week_start"] = (
        today - pd.to_timedelta(
            (weekly_summary["week_number"] - 1) * 7,
            unit="D"
        )
    )

    weekly_summary = weekly_summary.sort_values("week_start")

    # -----------------------
    # WoW Growth
    # -----------------------
    weekly_rev = weekly_summary.set_index("week")["total_revenue"]

    wow_growth = None

    if {"week-1", "week-2"}.issubset(weekly_rev.index):
        previous = weekly_rev["week-2"]
        current = weekly_rev["week-1"]

        if previous != 0:
            wow_growth = (current - previous) / previous
            logger.info(f"WoW growth calculated: {wow_growth:.4f}")
        else:
            logger.warning("Previous week revenue is zero. WoW growth undefined.")
    else:
        logger.warning("Insufficient weeks to calculate WoW growth.")

    # -----------------------
    # Weekly Orders
    # -----------------------
    if "order_id" in df.columns:
        weekly_orders = (
            df
            .groupby("week")
            .agg(total_orders=("order_id", "nunique"))
            .reset_index()
        )
        logger.info("Weekly orders calculated using unique order_id.")
    else:
        weekly_orders = (
            df
            .groupby("week")
            .size()
            .reset_index(name="total_orders")
        )
        logger.warning("order_id column missing. Using row count as total_orders.")

    weekly_orders["week_number"] = (
        weekly_orders["week"]
        .str.extract(r"(\d+)")
        .astype(int)
    )

    weekly_orders["week_start"] = (
        today - pd.to_timedelta(
            (weekly_orders["week_number"] - 1) * 7,
            unit="D"
        )
    )

    weekly_orders = weekly_orders.sort_values("week_start")

    logger.info(f"Weekly orders summary created. Rows: {len(weekly_orders)}")
    logger.info("one_month_analysis step completed successfully.")

    return {
        "weekly_summary": weekly_summary.round(2),
        "weekly_orders": weekly_orders,
        "wow_growth": round(wow_growth, 4) if wow_growth is not None else None
    }
