from pyspark.sql import SparkSession

def create_spark_session():
    """
    Create and return a Spark session.
    """
    return SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

def single_query_main():
    """
    Execute SQL query on the Delta table.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table_transformed"

    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        raise ValueError(f"Table {table_name} not found in {database}.")

    query = f"SELECT * FROM {database}.{table_name} LIMIT 10"
    result = spark.sql(query)
    result.show()
    print(f"Query executed on {table_name}")
