from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import logging
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# -------------------------------------------------
# Import your existing modules
# -------------------------------------------------
#from amazon.loading import loading
from amazon.clearning import clean_data
from amazon.prepare_data import prepare_reporting_data
from amazon.one_week_analysis import one_week_analysis
from amazon.one_month_analysis import one_month_analysis
from amazon.generate_pdf_aws import generate_pdf


# -------------------------------------------------
# Logging Configuration
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)


# -------------------------------------------------
# DEMO MODE CONFIGURATION
# -------------------------------------------------
DEMO_MODE = True  #  Change to False for real weekly automation
FORCED_DATE = datetime(2026, 2, 7)


# -------------------------------------------------
# Default DAG Arguments
# -------------------------------------------------
default_args = {
    "owner": "soichiro",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# -------------------------------------------------
# DAG Definition
# -------------------------------------------------
with DAG(
    dag_id="amazon_sales_report_etl_AWS",
    default_args=default_args,
    description="Weekly Amazon Sales Reporting ETL Pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["portfolio", "reporting"],
) as dag:


    # -------------------------------------------------
    # Task 1: Load Data
    # -------------------------------------------------
    @task
    def task_loading():
        hook = S3Hook(aws_conn_id="aws_default") 

        file_obj = hook.get_key(
            key="raw/amazon_sales_dataset.csv",
            bucket_name="soichiro-airflow-output-2026"
        )

        df = pd.read_csv(file_obj.get()['Body'])


        temp_path = "/tmp/raw_data.csv"
        df.to_csv(temp_path, index=False)

        return temp_path


    # -------------------------------------------------
    # Task 2: Clean Data
    # -------------------------------------------------
    @task
    def task_cleaning(file_path):
        logger.info("Task 2: Cleaning data")
  
        df = pd.read_csv(file_path)
        df = clean_data(df)

        temp_path = "/tmp/cleaned_data.csv"
        df.to_csv(temp_path, index=False)

        logger.info("Cleaning completed")
        return temp_path


    # -------------------------------------------------
    # Task 3a: Prepare Weekly Data
    # -------------------------------------------------
    @task
    def task_prepare_week(file_path, ds=None):

        df = pd.read_csv(file_path)

        if DEMO_MODE:
            today = FORCED_DATE
        else:
            today = datetime.strptime(ds, "%Y-%m-%d")

        one_week_df, _ = prepare_reporting_data(df, today)

        logger.info(f"Prepared weekly data: {len(one_week_df)} rows")

        temp_path = "/tmp/one_week.csv"
        one_week_df.to_csv(temp_path, index=False)


        return temp_path


    # -------------------------------------------------
    # Task 3b: Prepare Monthly Data
    # -------------------------------------------------
    @task
    def task_prepare_month(file_path, ds=None):

        df = pd.read_csv(file_path)


        if DEMO_MODE:
            today = FORCED_DATE
        else:
            today = datetime.strptime(ds, "%Y-%m-%d")

        _, one_month_df = prepare_reporting_data(df, today)

        logger.info(f"Prepared monthly data: {len(one_month_df)} rows")

        temp_path = "/tmp/one_month.csv"
        one_month_df.to_csv(temp_path, index=False)

        return temp_path


    # -------------------------------------------------
    # Task 4: One Week Analysis
    # -------------------------------------------------
    @task
    def task_one_week_analysis(file_path):
        logger.info("Task 4: Running one-week analysis")
        one_week_df = pd.read_csv(file_path)
        results = one_week_analysis(one_week_df)
        return results


    # -------------------------------------------------
    # Task 5: One Month Analysis
    # -------------------------------------------------
    @task
    def task_one_month_analysis(file_path, ds=None):

        one_month_df = pd.read_csv(file_path)

        if DEMO_MODE:
            today = FORCED_DATE
        else:
            today = datetime.strptime(ds, "%Y-%m-%d")

        logger.info("Task 5: Running one-month analysis")
        results = one_month_analysis(one_month_df, today)

        return results




    # -------------------------------------------------
    # Task 6: Generate PDF
    # -------------------------------------------------
    @task
    def task_generate_pdf(one_week_results, monthly_results, pdf_filename: str):
        logger.info("Task 6: Generating PDF report")

        file_paths = generate_pdf(
            pdf_path=pdf_filename,
            one_week_results={
                **one_week_results,
                "wow_growth": monthly_results["wow_growth"]
            },
            aggregation_df=one_week_results["aggregation"],
            weekly_summary=monthly_results["weekly_summary"],
            weekly_orders=monthly_results["weekly_orders"]
        )

        logger.info("PDF generation completed successfully")

        return file_paths
    
    
    

    # -------------------------------------------------
    # Task 7: Upload PDF
    # -------------------------------------------------
    @task
    def task_upload_to_s3(file_paths: dict):
        hook = S3Hook(aws_conn_id="aws_default")
        bucket = "soichiro-airflow-output-2026"

        for name, path in file_paths.items():

            filename = path.split("/")[-1]
            s3_key = f"reports/{name}/{filename}"

            hook.load_file(
                filename=path,
                key=s3_key,
                bucket_name=bucket,
                replace=True
            )

            logger.info(f"Uploaded {name} → s3://{bucket}/{s3_key}")

    # -------------------------------------------------
    # File Paths
    # -------------------------------------------------
    #file_path = "/opt/airflow/dags/amazon_sales_dataset.csv"
    
   # PDF_file_path = "Weekly_Performance_Report.pdf"

    pdf_filename = "Weekly_Performance_Report.pdf"


    # -------------------------------------------------
    # Task Dependencies
    # -------------------------------------------------

    temp_path = task_loading()
    temp_path = task_cleaning(temp_path)

    one_week_path = task_prepare_week(temp_path)      
    one_month_path = task_prepare_month(temp_path)    

    one_week_results = task_one_week_analysis(one_week_path)
    monthly_results = task_one_month_analysis(one_month_path)

    pdf_files = task_generate_pdf(one_week_results, monthly_results, pdf_filename)

    task_upload_to_s3(pdf_files)


