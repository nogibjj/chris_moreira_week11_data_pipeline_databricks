from pyspark.sql import functions as F

def transform_data(df):
    """
    Transforms the input Spark DataFrame by adding a derived column
    to categorize the popularity of tracks.

    Args:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        DataFrame: The transformed Spark DataFrame.
    """
    # Add a new column categorizing track popularity
    transformed_df = df.withColumn(
        "popularity_category",
        F.when(F.col("streams") > 1000000000, "Ultra Popular")
         .when((F.col("streams") > 500000000) & (F.col("streams") <= 1000000000), "Very Popular")
         .when((F.col("streams") > 100000000) & (F.col("streams") <= 500000000), "Popular")
         .otherwise("Less Popular")
    )
    return transformed_df
