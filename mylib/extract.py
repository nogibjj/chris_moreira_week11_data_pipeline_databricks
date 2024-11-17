import os
import pandas as pd
from pyspark.sql import SparkSession

def extract(url, table_name, database="my_database"):
    """
    Extracts data from a URL, cleans the column names, and writes to a Delta table.

    Args:
        url (str): The URL of the CSV file to download.
        table_name (str): The name of the Delta table to create.
        database (str): The Databricks Catalog database to save the table in.

    Returns:
        None
    """
    # Initialize Spark session
    spark = SparkSession.builder.appName("Read CSV").getOrCreate()

    # Load data from URL into a Pandas DataFrame
    df = pd.read_csv(url)

    # Clean column names (remove invalid characters)
    df.columns = [col.strip().replace(" ", "_").replace(";", "").replace("\n", "").replace("\t", "").replace("(", "").replace(")", "") for col in df.columns]

    # Convert the Pandas DataFrame to a Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Write the Spark DataFrame to Delta format
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark_df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.{table_name}")

    print(f"Data successfully loaded into Delta table: {database}.{table_name}")
