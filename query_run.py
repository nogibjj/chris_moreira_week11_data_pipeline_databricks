from pyspark.sql import SparkSession

def run_query():
    spark = SparkSession.builder \
        .appName("Spotify_Query") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.1") \
        .getOrCreate()

    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table"

    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        raise ValueError(f"Table {table_name} not found.")
    query = f"""
        SELECT artist_name, SUM(streams) AS total_streams
        FROM {database}.{table_name}
        GROUP BY artist_name
    """
    result = spark.sql(query)
    result.show()
