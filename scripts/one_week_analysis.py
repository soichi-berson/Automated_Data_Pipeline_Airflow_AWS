
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def one_week_analysis(one_week_df: pd.DataFrame) -> dict:
    """
    Perform one-week KPI analysis.

    Returns:
        dict containing:
            - total_revenue
            - gross_revenue
            - total_orders
            - aov
            - aggregation (DataFrame)
    """

    logger.info("Starting one_week_analysis step.")

    if one_week_df.empty:
        logger.warning("one_week_df is empty. Returning zero metrics.")

    required_cols = ["total_sales", "gross_sales", "quantity", "sub_category"]
    missing_cols = [col for col in required_cols if col not in one_week_df.columns]

    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = one_week_df.copy()

    # --- KPI Calculations ---
    total_revenue = df["total_sales"].sum()
    gross_revenue = df["gross_sales"].sum()
    total_orders = len(df)

    aov = total_revenue / total_orders if total_orders > 0 else 0

    logger.info(
        f"KPI calculated: revenue={total_revenue:.2f}, "
        f"orders={total_orders}, aov={aov:.2f}"
    )

    # --- Sub-category Aggregation ---
    aggregation = (
        df
        .groupby("sub_category")
        .agg(
            total_quantity=("quantity", "sum"),
            total_revenue=("total_sales", "sum")
        )
        .reset_index()
    )

    aggregation["total_revenue"] = aggregation["total_revenue"].round(2)

    aggregation_sorted = aggregation.sort_values(
        by="total_revenue",
        ascending=False
    )

    logger.info("Sub-category aggregation completed.")
    logger.info("one_week_analysis step completed successfully.")

    return {
        "total_revenue": round(total_revenue, 2),
        "gross_revenue": round(gross_revenue, 2),
        "total_orders": total_orders,
        "aov": round(aov, 2),
        "aggregation": aggregation_sorted
    }