from pyspark.sql import SparkSession

def transform_data(database, table_name):
    """
    Transform data in Delta table.

    Args:
        database (str): Databricks database.
        table_name (str): Name of the Delta table.

    Returns:
        None
    """
    spark = SparkSession.builder.getOrCreate()

    # Transform data by adding a "transformed" column
    query = f"""
        CREATE OR REPLACE TABLE {database}.{table_name}_transformed
        AS SELECT *, 1 AS transformed FROM {database}.{table_name}
    """
    spark.sql(query)
