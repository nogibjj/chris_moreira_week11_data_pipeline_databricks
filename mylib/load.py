from pyspark.sql import SparkSession

def load_data(database, transformed_table):
    """
    Load transformed data into a destination system.
    """
    spark = SparkSession.builder.getOrCreate()
    data = spark.sql(f"SELECT * FROM {database}.{transformed_table}")
    data.show()
