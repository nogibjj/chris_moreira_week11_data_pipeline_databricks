from pyspark.sql import SparkSession

def load_data(database, table_name):
    """
    Load transformed data from Delta table.

    Args:
        database (str): Databricks database.
        table_name (str): Name of the transformed Delta table.

    Returns:
        None
    """
    spark = SparkSession.builder.getOrCreate()

    # Load transformed data
    query = f"SELECT * FROM {database}.{table_name}_transformed"
    data = spark.sql(query)
    print(f"Loaded data:\n{data.show()}")
