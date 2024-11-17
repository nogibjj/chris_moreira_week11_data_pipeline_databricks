"""
Extract data from a URL, clean it, and save as a Delta table.
"""

import pandas as pd
from pyspark.sql import SparkSession

def extract(url, table_name, database="csm_87_database"):
    """
    Extracts data from a URL, cleans column names, and writes to Delta.

    Args:
        url (str): The URL of the CSV file to download.
        table_name (str): The name of the Delta table to create.
        database (str): The Databricks Catalog database.

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

    # Convert the Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Write Spark DataFrame to Delta format
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{database}.{table_name}"
    )
    print(f"Data loaded into Delta table: {database}.{table_name}")
