from pyspark.sql import SparkSession
import pandas as pd

def extract(url, table_name, database="csm_87_database"):
    spark = SparkSession.builder \
        .appName("Spotify_ETL") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    try:
        # Load data into Pandas DataFrame
        df = pd.read_csv(url)
        print("Data loaded successfully.")

        # Clean column names
        df.columns = [col.strip().replace(" ", "_") for col in df.columns]
        print("Column names cleaned.")

        # Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df)

        # Create database if not exists
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

        # Write Spark DataFrame to Delta table
        spark_df.write.format("delta").mode("overwrite").saveAsTable(
            f"{database}.{table_name}"
        )
        print(f"Data successfully written to {database}.{table_name}")

    except Exception as e:
        print(f"Error in extract process: {e}")
