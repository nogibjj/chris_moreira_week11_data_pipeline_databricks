from pyspark.sql import SparkSession


def load_data(database, table_name):
    """
    Load transformed data into its final destination.

    Args:
        database (str): Databricks database.
        table_name (str): Name of the table to load.

    Returns:
        None
    """
    spark = SparkSession.builder.getOrCreate()
    spark.sql(f"SELECT * FROM {database}.{table_name}").show()
