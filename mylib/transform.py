from pyspark.sql import SparkSession


def transform_data(database, raw_table, transformed_table):
    """
    Transform raw data table into a cleaned and enriched version.

    Args:
        database (str): Databricks database.
        raw_table (str): Name of the raw data table.
        transformed_table (str): Name of the transformed table.

    Returns:
        None
    """
    spark = SparkSession.builder.getOrCreate()

    query = f"""
    CREATE OR REPLACE TABLE {database}.{transformed_table} AS
    SELECT
        track_name AS TrackName,
        artists_name AS Artist,
        streams AS Streams,
        released_year AS ReleaseYear
    FROM {database}.{raw_table}
    WHERE artists_name IS NOT NULL
    """
    spark.sql(query)
