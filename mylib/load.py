"""
Load transformed data into a Delta table.
"""

def load_data(df, table_name, database="csm_87_database"):
    """
    Loads the transformed DataFrame into a Delta table.

    Args:
        df (DataFrame): The transformed Spark DataFrame.
        table_name (str): The name of the Delta table.
        database (str): The Databricks Catalog database.

    Returns:
        None
    """
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{database}.{table_name}"
    )
    print(f"Transformed data loaded into table: {database}.{table_name}")
