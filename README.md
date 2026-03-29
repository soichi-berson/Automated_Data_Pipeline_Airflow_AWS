# Automated Data Pipeline for E-commerce Sales Reporting

## Introduction
This project demonstrates the design and implementation of an automated data pipeline for analysing e-commerce sales data.  
It is developed using Python and showcases the use of Apache Airflow and AWS S3 for workflow orchestration and cloud-based storage.

## Data Source
Kaggle (available from Kaggle)

E-commerce Sales Dataset:  
https://www.kaggle.com/datasets/sharmajicoder/e-commerce-sales-dataset

## Objective
The objective of this project is to automate the generation of weekly sales reports by replacing manual data processing with a structured ETL pipeline.

## Features
- Automated ETL pipeline using Apache Airflow  
- Weekly KPI reporting (Revenue, Orders, AOV, WoW growth)  
- Data visualisation using Matplotlib  
- PDF report generation  
- AWS S3 integration for storage  
- Modular pipeline design  

## Project Structure
.
├── scripts/                             # ETL processing scripts
├── amazon_sales_reporting_etl_AWS.py    # Airflow DAG
├── Automated_Data_Pipeline_Airflow_AWS.pdf   # Project report
├── Example_Weekly_Performance_Report.pdf     # Example output
└── README.md

## How to Run

### 1. Start Airflow
docker compose up

### 2. Access Airflow UI
http://localhost:8080

### 3. Trigger DAG
airflow dags trigger amazon_sales_report_etl_AWS

## Example Output
The pipeline generates a weekly report including:
- KPI summary  
- Sub-category analysis  
- Revenue trends  
- Order trends  

See: Example_Weekly_Performance_Report.pdf

## Tech Stack
- Python (Pandas, Matplotlib, ReportLab)  
- Apache Airflow  
- AWS S3 & IAM  
- Docker  

## License
This project is licensed under the MIT License.

## Author
Soichiro Tanabe

Feel free to explore the project and reach out if you have any questions.
