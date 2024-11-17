from pyspark.sql import SparkSession
from mylib.extract import extract
from mylib.transform import transform_data
from mylib.load import load_data

def create_spark_session(app_name="Spotify_ETL"):
    """Create Spark session with Delta support."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.5.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return builder.getOrCreate()

def main():
    """Orchestrate the ETL process."""
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table"
    url = ("https://raw.githubusercontent.com/nogibjj/"
           "chris_moreira_week6_sql_databricks/main/data/"
           "Spotify_Most_Streamed_Songs.csv")

    print("Starting extract step...")
    extract(url, table_name, database)

    print("Starting transform step...")
    spark.sql(f"USE {database}")
    df = spark.table(f"{database}.{table_name}")
    transformed_df = transform_data(df)

    print("Starting load step...")
    load_data(transformed_df, table_name, database)

    print("ETL process completed successfully!")

if __name__ == "__main__":
    main()
