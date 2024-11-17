from pyspark.sql import SparkSession

def single_query_main():
    """
    Execute SQL query on the Delta table.
    """
    spark = SparkSession.builder.getOrCreate()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table_transformed"

    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        raise ValueError(f"Table {table_name} not found in {database}.")

    result = spark.sql(
        f"SELECT Artist, COUNT(*) AS SongCount "
        f"FROM {database}.{table_name} "
        "GROUP BY Artist ORDER BY SongCount DESC LIMIT 10"
    )
    result.show()
