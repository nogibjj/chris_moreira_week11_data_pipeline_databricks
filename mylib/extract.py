"""
Extract data from a URL, clean it, and save as a Delta table.
"""

import pandas as pd
from pyspark.sql import SparkSession

def extract(url, table_name, database="csm_87_database"):
    """
    Extract data from URL, clean, and save to Delta table.

    Args:
        url (str): The URL for the CSV file to download.
        table_name (str): The name of the Delta table to create.
        database (str): The Databricks Catalog database.

    Returns:
        None
    """
    spark = SparkSession.builder.getOrCreate()

    # Load CSV data into Pandas DataFrame
    df = pd.read_csv(url)

    # Clean column names by removing spaces and special characters
    df.columns = [
        col.strip().replace(" ", "_").replace("(", "").replace(")", "")
        for col in df.columns
    ]

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Create database if it doesn't exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Save the DataFrame as a Delta table in Databricks
    spark_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{database}.{table_name}"
    )
    print(f"Data loaded to Delta table: {database}.{table_name}")
