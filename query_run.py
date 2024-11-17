"""
Run an insightful SQL query on the Spotify dataset using PySpark.
"""

from pyspark.sql import SparkSession


def create_spark_session(app_name="Spark Application"):
    """
    Creates a Spark session, using Databricks shared SparkSession if available.
    """
    if SparkSession.getActiveSession():
        print("Detected Databricks runtime. Using shared SparkSession.")
        return SparkSession.getActiveSession()
    else:
        print("Running in local mode.")
        return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()


def single_query_main():
    """
    Executes a predefined SQL query on the Spotify dataset.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table"

    query = f"""
    SELECT 
        artists_name AS artist_name, 
        SUM(CAST(streams AS BIGINT)) AS total_streams,
        ROUND(AVG(`danceability_%`), 2) AS avg_danceability,
        ROUND(AVG(`energy_%`), 2) AS avg_energy
    FROM {database}.{table_name}
    GROUP BY artists_name
    ORDER BY total_streams DESC
    LIMIT 5
    """

    result_df = spark.sql(query)
    result_df.show(5, truncate=False)


if __name__ == "__main__":
    single_query_main()
