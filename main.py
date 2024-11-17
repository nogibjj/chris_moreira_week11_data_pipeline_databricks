"""
Main ETL pipeline entry point for Databricks Task
"""

from pyspark.sql import SparkSession
from mylib.extract import extract
from mylib.transform import transform_data
from mylib.load import load_data


def create_spark_session(app_name="Databricks ETL Pipeline"):
    """
    Creates a Spark session, using Databricks shared SparkSession if available.
    """
    if SparkSession.getActiveSession():
        print("Detected Databricks runtime. Using shared SparkSession.")
        return SparkSession.getActiveSession()
    else:
        print("Running in local mode.")
        return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()


def main():
    """
    Orchestrates the ETL process: extract, transform, and load data.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table"
    url = (
        "https://raw.githubusercontent.com/nogibjj/"
        "chris_moreira_week6_sql_databricks/refs/heads/"
        "main/data/Spotify_Most_Streamed_Songs.csv"
    )

    print("Starting extract step...")
    extract(url, table_name, database)
    print("Extract step completed successfully.")

    print("Starting transform step...")
    df = spark.table(f"{database}.{table_name}")
    transformed_df = transform_data(df)
    print("Transform step completed successfully.")

    print("Starting load step...")
    load_data(transformed_df, table_name, database)
    print("Load step completed successfully.")

    print("ETL pipeline executed successfully.")


if __name__ == "__main__":
    main()
