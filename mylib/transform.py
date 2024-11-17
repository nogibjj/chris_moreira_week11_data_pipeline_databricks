from pyspark.sql import SparkSession

def transform_data(database, raw_table, transformed_table):
    """
    Transform raw data into transformed data.

    Args:
        database (str): The database name.
        raw_table (str): The name of the raw table.
        transformed_table (str): The name of the transformed table.
    """
    spark = SparkSession.builder.getOrCreate()
    
    # Drop the transformed table if it already exists
    spark.sql(f"DROP TABLE IF EXISTS {database}.{transformed_table}")
    
    # Create the transformed table
    query = (
        f"CREATE TABLE {database}.{transformed_table} AS "
        f"SELECT * FROM {database}.{raw_table} "
        f"WHERE artists_name IS NOT NULL"
    )
    spark.sql(query)
