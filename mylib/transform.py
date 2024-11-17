"""
Perform transformations on a Spark DataFrame.
"""

from pyspark.sql import functions as F


def transform_data(df):
    """
    Transforms the input Spark DataFrame by adding a derived column.

    Args:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        DataFrame: The transformed Spark DataFrame.
    """
    transformed_df = df.withColumn(
        "popularity_category",
        F.when(F.col("streams") > 1_000_000_000, "Ultra Popular")
        .when(
            (F.col("streams") > 500_000_000) &
            (F.col("streams") <= 1_000_000_000),
            "Very Popular",
        )
        .when(
            (F.col("streams") > 100_000_000) &
            (F.col("streams") <= 500_000_000),
            "Popular",
        )
        .otherwise("Less Popular"),
    )

    return transformed_df
