"""
Load transformed data into a Delta table.
"""


def load_data(df, table_name, database="my_database"):
    """
    Loads the transformed Spark DataFrame into a Delta table.

    Args:
        df (DataFrame): The transformed Spark DataFrame.
        table_name (str): The name of the Delta table to update.
        database (str): The Databricks Catalog database containing the table.

    Returns:
        None
    """
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{database}.{table_name}"
    )
    print(f"Transformed data loaded into table: {database}.{table_name}")
