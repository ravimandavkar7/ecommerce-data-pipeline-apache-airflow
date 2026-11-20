# E-commerce Data Engineering Pipeline using Apache Airflow

## Project Overview

This project implements an end-to-end E-commerce Data Engineering pipeline using Apache Airflow, Python and SQL Server

The pipeline extracts customer and e-commerce data from Microsoft SQL Server, performs data cleaning and transformation using Python and Pandas, and loads the processed data into SQL.

Apache Airflow orchestrates the ETL workflow, manages task dependencies, and enables incremental data processing.

## Architecture

**SQL Server → Python (ETL) → Apache Airflow → SQL server**

## Technologies Used

- **Python:** Data extraction, transformation, and loading.
- **Apache Airflow 3.3.2:** Workflow orchestration and task scheduling.
- **Microsoft SQL Server:** Source database.
- **Pandas:** Data cleaning and transformation.
- **PyODBC:** SQL Server connectivity.
- **Docker & Docker Compose:** Airflow environment setup.
- **Logging:** Pipeline execution monitoring and error tracking.

## Key Features

- End-to-end ETL pipeline implementation.
- SQL Server data extraction using Python.
- Incremental data loading using watermark/version tracking.
- Data cleaning and transformation using Pandas.
- Duplicate record identification and handling.
- Null value validation and data quality checks.
- Separation of valid and invalid records.
- Loading processed data into SQL.
- Watermark management for incremental processing.
- Error handling and exception management.
- Logging for pipeline execution and debugging.
- Apache Airflow DAG orchestration with task dependencies.

## ETL Workflow

### 1. Extract
- Connect to Microsoft SQL Server using PyODBC.
- Retrieve source data from the E-commerce database.
- Identify the previous and current watermark values.
- Extract only new or updated records during incremental execution.

### 2. Transform
- Process extracted data using Python and Pandas.
- Handle missing and null values.
- Identify and remove duplicate records.
- Perform data validation.
- Separate valid and invalid records.

### 3. Load
- Connect to SQL.
- Load valid records into the target table.
- Store invalid records separately for further investigation.
- Update the watermark after successful processing.

### 4. Orchestration
- Create Apache Airflow DAGs to orchestrate the ETL workflow.
- Define task dependencies between extraction, transformation, and loading.
- Monitor task execution through the Airflow UI.
- Handle task failures and execution logs.

## Project Structure

```text
ecommerce-data-pipeline-apache-airflow/
│
├── dags/
│   └── ecommerce_dag.py
│
├── ecommerce/
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── logger.py
│
├── config/
│   └── config.ini
│
├── docker-compose.yaml
│
├── requirements.txt
│
└── README.md
```

## Prerequisites

- Python 3.x
- Docker Desktop
- Apache Airflow
- Microsoft SQL Server
- SQL Server ODBC Driver
- Required Python libraries

## Configuration

Update the database connection details in `config.ini`.

Configure the following:

- SQL Server connection details.
- Database, schema, warehouse, and target table information.

## Execution

1. Clone the repository.
2. Configure the SQL Server.
3. Install the required dependencies.
4. Start the Airflow environment using Docker Compose.
5. Access the Airflow UI.
6. Enable and trigger the E-commerce DAG.
7. Monitor task execution and verify the loaded data in SQL.

## Learning Outcomes

- Practical understanding of ETL pipeline development.
- Hands-on experience with Apache Airflow DAGs and task orchestration.
- Implementation of incremental data loading.
- Data transformation and validation using Python and Pandas.
- Integration of SQL Server.
- Understanding of data quality checks and error handling.
- Experience with Docker-based Airflow deployment.

## Author

Developed as a hands-on Data Engineering project to demonstrate ETL development, workflow orchestration, incremental data processing, and cloud data warehousing.
