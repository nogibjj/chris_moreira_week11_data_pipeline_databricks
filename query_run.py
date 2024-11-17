"""
Run an insightful SQL query on the Spotify dataset using PySpark.
"""

from pyspark.sql import SparkSession

def create_spark_session(app_name="Spotify_Query"):
    """
    Creates a Spark session.

    Returns:
        SparkSession: The created or active session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def single_query_main():
    """
    Executes a predefined SQL query on the Spotify dataset.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table"

    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        raise ValueError(f"Table {table_name} not found in {database}.")

    query = f"""
    SELECT artists_name AS artist_name, 
           SUM(CAST(streams AS BIGINT)) AS total_streams,
           ROUND(AVG(`danceability_%`), 2) AS avg_danceability,
           ROUND(AVG(`energy_%`), 2) AS avg_energy
    FROM {database}.{table_name}
    GROUP BY artists_name
    ORDER BY total_streams DESC
    LIMIT 5
    """
    result_df = spark.sql(query)
    result_df.show()

if __name__ == "__main__":
    single_query_main()
