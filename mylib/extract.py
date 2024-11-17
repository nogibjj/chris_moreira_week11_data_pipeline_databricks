"""
Extract data from a URL, clean it, and save as a Delta table.
"""

import pandas as pd
from pyspark.sql import SparkSession

def extract(url, table_name, database="csm_87_database"):
    """
    Extract data from URL, clean, and save to Delta.

    Args:
        url (str): CSV file URL.
        table_name (str): Name of Delta table to create.
        database (str): Databricks database.

    Returns:
        None
    """
    spark = SparkSession.builder.getOrCreate()

    # Load data into Pandas DataFrame
    df = pd.read_csv(url)

    # Clean column names
    df.columns = [
        col.strip().replace(" ", "_").replace("(", "").replace(")", "")
        for col in df.columns
    ]

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Write Spark DataFrame to Delta table
    spark_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{database}.{table_name}"
    )
    print(f"Data loaded into Delta table: {database}.{table_name}")
