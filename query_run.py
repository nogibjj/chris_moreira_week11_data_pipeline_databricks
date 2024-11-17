"""
Run a single SQL query on the Delta table.
"""

from pyspark.sql import SparkSession

def create_spark_session():
    """
    Create a Spark session with Delta support.
    """
    builder = (
        SparkSession.builder.appName("QueryRun")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.1")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return builder.getOrCreate()

def single_query_main():
    """
    Execute SQL query on the Delta table.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table_transformed"

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
    result = spark.sql(query)
    result.show()
