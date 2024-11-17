[![CI Pipeline](https://github.com/nogibjj/chris_moreira_week11_data_pipeline_databricks/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/chris_moreira_week11_data_pipeline_databricks/actions/workflows/cicd.yml)

# Project Goal
This project is designed to demonstrate the end-to-end process of building a data pipeline in Databricks. The pipeline extracts data from a CSV source, transforms it to include meaningful insights like popularity categories, and loads the transformed data into a Delta table. The processed data is then queried and visualized to generate actionable insights. This project emphasizes the application of modern ETL workflows, data visualization techniques, and automation using GitHub CI/CD integration.


# Pyspark Functions running in the following order:
- extract.py: extract(url, table_name, database): Extracts the data from a given CSV URL, cleans it by renaming columns, and loads the data into a Delta table within the specified Databricks database.
- transform.py: transform_data(spark, database, raw_table, transformed_table): Reads the raw data from a Delta table, adds a new column (popularity_category) based on the song popularity score, and saves the transformed data into a new Delta table.
- load.py load_data(database, table_name): Ensures the transformed data is properly loaded into the specified Delta table in the database.
- query_run.py: single_query_main(): Runs a sample SQL query on the transformed Delta table, fetching a limited number of rows for previewing data insights.
- viz.py: run_viz(): Queries the transformed Delta table and generates a bar chart visualization for analyzing popularity categories using Matplotlib.
- main.py: main(): Orchestrates the ETL process by calling the extract, transform_data, and load_data functions sequentially. This is the central script for running the pipeline.
- test_main.py: Contains basic test cases to ensure the individual components of the pipeline work as expected. It checks for issues in the ETL flow and query execution.

# Repository Schema
![image](https://github.com/user-attachments/assets/9ca61869-995a-489f-9029-0a7318591dee)


# Databricks Workflow
![image](https://github.com/user-attachments/assets/161b503f-8cd2-413f-8178-4016c777c7cc)


# Project Structure
![image](https://github.com/user-attachments/assets/06ed4ad7-2651-410a-b2e3-6d19a3e3b79c)


## Results Snapshot ##
# Databricks Workflow
![image](https://github.com/user-attachments/assets/161b503f-8cd2-413f-8178-4016c777c7cc)

# Database & Data Table Creation
![image](https://github.com/user-attachments/assets/28a03cff-f9cb-41a5-95e6-e5a3488b81d5)

# Data Visual
![image](https://github.com/user-attachments/assets/cdf6c2ee-7bfb-4104-b74f-1605cdb3bef1)
