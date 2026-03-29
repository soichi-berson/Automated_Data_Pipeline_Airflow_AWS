
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer,
    Table, TableStyle, Image
)
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import pagesizes
from reportlab.lib.units import inch
import matplotlib.pyplot as plt
import os
import logging
from datetime import datetime
logger = logging.getLogger(__name__)




def generate_pdf(
    pdf_path: str,
    one_week_results: dict,
    aggregation_df,
    weekly_summary,
    weekly_orders
) -> None:
    """
    Generate executive weekly performance PDF report.
    """

    logger.info("Starting PDF generation process.")

    try:
        # -----------------------------
        # Unpack results
        # -----------------------------
        Total_Revenue = one_week_results["total_revenue"]
        Gross_Revenue = one_week_results["gross_revenue"]
        aov = one_week_results["aov"]
        wow_growth = one_week_results.get("wow_growth")

        styles = getSampleStyleSheet()
        elements = []


        # -------------------------------------------------
        # Setup output directory 
        # -------------------------------------------------

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        BASE_PATH = "/tmp"
        os.makedirs(BASE_PATH, exist_ok=True)

        # Force PDF path inside container
        base_dir = BASE_PATH  

        pdf_filename = os.path.basename(pdf_path)
        pdf_path = os.path.join(base_dir, f"{timestamp}_{pdf_filename}")

        doc = SimpleDocTemplate(pdf_path, pagesize=pagesizes.A4)

        # Save all charts in same folder
        subcat_chart_path = os.path.join(base_dir, f"subcategory_chart_{timestamp}.png")
        rev_chart_path = os.path.join(base_dir, f"revenue_trend_{timestamp}.png")
        orders_chart_path = os.path.join(base_dir, f"orders_trend_{timestamp}.png")

        # -----------------------------
        # Save charts
        # -----------------------------
        logger.info("Generating subcategory chart.")
        plt.figure()
        plt.bar(
            aggregation_df["sub_category"],
            aggregation_df["total_revenue"]
        )
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(subcat_chart_path)
        plt.close()

        logger.info("Generating revenue trend chart.")
        plt.figure()
        plt.plot(
            weekly_summary["week_start"],
            weekly_summary["total_revenue"],
            marker="o"
        )
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(rev_chart_path)
        plt.close()

        logger.info("Generating orders trend chart.")
        plt.figure()
        plt.plot(
            weekly_orders["week_start"],
            weekly_orders["total_orders"],
            marker="o"
        )
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(orders_chart_path)
        plt.close()

        # -----------------------------
        # Build PDF content
        # -----------------------------
        logger.info("Building PDF content.")

        elements.append(
            Paragraph("Executive Weekly Performance Report", styles["Heading1"])
        )
        elements.append(Spacer(1, 0.3 * inch))

        # KPI table
        elements.append(Paragraph("One Week KPI Summary", styles["Heading2"]))
        elements.append(Spacer(1, 0.2 * inch))

        kpi_data = [
            ["Metric", "Value"],
            ["Total Revenue", f"{Total_Revenue:,.2f}"],
            ["Gross Revenue", f"{Gross_Revenue:,.2f}"],
            ["AOV", f"{aov:,.2f}"],
            ["WoW Growth", f"{wow_growth:.2%}" if wow_growth is not None else "N/A"],
        ]

        kpi_table = Table(kpi_data)
        kpi_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ]))

        elements.append(kpi_table)
        elements.append(Spacer(1, 0.4 * inch))

        # Subcategory chart
        elements.append(
            Paragraph("Sub-Category Revenue (Last 7 Days)", styles["Heading2"])
        )
        elements.append(Spacer(1, 0.2 * inch))
        elements.append(Image(subcat_chart_path, width=5*inch, height=3*inch))
        elements.append(Spacer(1, 0.4 * inch))

        # Subcategory table
        elements.append(
            Paragraph("Sub-Category Performance (Last 7 Days)", styles["Heading2"])
        )
        elements.append(Spacer(1, 0.2 * inch))

        agg_data = [["Sub Category", "Total Quantity", "Total Revenue"]]

        for _, row in aggregation_df.iterrows():
            agg_data.append([
                row["sub_category"],
                int(row["total_quantity"]),
                f"{row['total_revenue']:,.2f}"
            ])

        agg_table = Table(agg_data)
        agg_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
        ]))

        elements.append(agg_table)
        elements.append(Spacer(1, 0.4 * inch))

        # Revenue trend
        elements.append(Paragraph("4-Week Revenue Trend", styles["Heading2"]))
        elements.append(Spacer(1, 0.2 * inch))
        elements.append(Image(rev_chart_path, width=5*inch, height=3*inch))
        elements.append(Spacer(1, 0.4 * inch))

        # Orders trend
        elements.append(Paragraph("4-Week Orders Trend", styles["Heading2"]))
        elements.append(Spacer(1, 0.2 * inch))
        elements.append(Image(orders_chart_path, width=5*inch, height=3*inch))

        # -----------------------------
        # Build file
        # -----------------------------

        doc.build(elements)

        logger.info(f"PDF successfully generated at: {pdf_path}")

        return {
            "pdf": pdf_path,
            "subcategory_chart": subcat_chart_path,
            "revenue_chart": rev_chart_path,
            "orders_chart": orders_chart_path
        }


    except Exception:
        logger.exception("PDF generation failed.")
        raise
