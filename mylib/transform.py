"""
Transform data.
"""

from pyspark.sql import functions as F

def transform_data(df):
    """
    Add popularity_category to data.
    """
    return df.withColumn(
        "popularity_category",
        F.when(F.col("streams") > 1_000_000_000, "Ultra Popular")
        .when((F.col("streams") > 500_000_000) & 
              (F.col("streams") <= 1_000_000_000), "Very Popular")
        .when((F.col("streams") > 100_000_000) & 
              (F.col("streams") <= 500_000_000), "Popular")
        .otherwise("Less Popular")
    )
