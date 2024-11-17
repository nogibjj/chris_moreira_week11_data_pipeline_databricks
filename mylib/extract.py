from pyspark.sql import SparkSession
import pandas as pd

def extract(url, table_name, database="csm_87_database"):
    """
    Extract data from a URL and save to a Delta table.
    """
    spark = SparkSession.builder.getOrCreate()
    df = pd.read_csv(url)
    df.columns = [
        col.strip().replace(" ", "_").replace("(", "").replace(")", "")
        for col in df.columns
    ]
    spark_df = spark.createDataFrame(df)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark_df.write.format("delta").mode("overwrite").saveAsTable(
        f"{database}.{table_name}"
    )
