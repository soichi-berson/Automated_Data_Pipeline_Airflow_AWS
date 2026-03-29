# Airflow & AWS Data Pipeline for E-commerce Sales Reporting

## Introduction
This project demonstrates the design and implementation of an automated data pipeline for analysing e-commerce sales data.  
It is developed using Python and showcases the use of Apache Airflow and AWS S3 for workflow orchestration and cloud-based storage.

## Data Source
Dataset: [E-commerce Sales Dataset (Kaggle)](https://www.kaggle.com/datasets/sharmajicoder/e-commerce-sales-dataset)

## Objective
The objective of this project is to automate the generation of weekly sales reports by replacing manual data processing with a structured ETL pipeline.

## Features
- Automated ETL pipeline using Apache Airflow  
- Weekly KPI reporting (Revenue, Orders, AOV, WoW growth)  
- Data visualisation using Matplotlib  
- PDF report generation  
- AWS S3 integration for data ingestion and storage, with secure access management using IAM  
- Modular pipeline design with separated ETL components  

## Documentation
For a detailed explanation of the system design, data processing workflow, and analysis, please refer to the full project report:
- `Automated_Data_Pipeline_Airflow_AWS.pdf`

## Material
- `scripts/`: ETL processing scripts  
- `amazon_sales_reporting_etl_AWS.py`: Airflow DAG definition  
- `Automated_Data_Pipeline_Airflow_AWS.pdf`: Full project report  
- `Example_Weekly_Performance_Report.pdf`: Example output report  
- `requirements.txt`: List of Python dependencies for reproducibility

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
