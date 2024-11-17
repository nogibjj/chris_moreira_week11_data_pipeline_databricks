from pyspark.sql import SparkSession

def main():
    """
    Query and visualize data.
    """
    spark = SparkSession.builder.getOrCreate()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table_transformed"

    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        raise ValueError(f"Table {table_name} not found in {database}.")

    # Query the data
    query = f"SELECT * FROM {database}.{table_name} LIMIT 10"
    data = spark.sql(query)
    print("Visualization:\n")
    data.show()
