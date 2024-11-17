from pyspark.sql import SparkSession
from mylib.extract import extract
from mylib.transform import transform_data
from mylib.load import load_data

def create_spark_session():
    """
    Create and configure a Spark session.

    Returns:
        SparkSession: Configured Spark session.
    """
    return (
        SparkSession.builder.appName("Spotify_ETL")
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
    )

def main():
    """
    Orchestrate ETL process.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table"
    url = (
        "https://raw.githubusercontent.com/nogibjj/"
        "chris_moreira_week6_sql_databricks/main/data/"
        "Spotify_Most_Streamed_Songs.csv"
    )

    print("Extracting data...")
    extract(url, table_name, database)

    print("Transforming data...")
    transform_data(database, table_name)

    print("Loading data...")
    load_data(database, table_name)

if __name__ == "__main__":
    main()
