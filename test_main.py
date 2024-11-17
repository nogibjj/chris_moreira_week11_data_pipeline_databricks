from main import main as run_main
from query_run import single_query_main as run_query
from viz import main as run_viz
from pyspark.sql import SparkSession

def setup_tables():
    """
    Setup required Delta tables for tests.
    """
    spark = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table"
    transformed_table = "csm_87_Spotify_Table_transformed"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        data = [("Test Artist", 123456)]
        columns = ["Artist", "Stream_Count"]
        spark_df = spark.createDataFrame(data, columns)
        spark_df.write.format("delta").mode("overwrite").saveAsTable(
            f"{database}.{table_name}"
        )
    if not spark.catalog.tableExists(f"{database}.{transformed_table}"):
        spark.sql(f"""
        CREATE TABLE {database}.{transformed_table} AS
        SELECT Artist, SUM(Stream_Count) AS Total_Streams
        FROM {database}.{table_name}
        GROUP BY Artist
        """)

setup_tables()

def test_main():
    """
    Test main.py execution.
    """
    try:
        run_main()
    except Exception as e:
        raise AssertionError(f"main.py failed: {e}")

def test_query():
    """
    Test query_run.py execution.
    """
    try:
        run_query()
    except Exception as e:
        raise AssertionError(f"query_run.py failed: {e}")

def test_viz():
    """
    Test viz.py execution.
    """
    try:
        run_viz()
    except Exception as e:
        raise AssertionError(f"viz.py failed: {e}")
