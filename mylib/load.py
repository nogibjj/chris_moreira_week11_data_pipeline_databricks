def load_data(df, table_name, database="my_database"):
    """
    Loads the transformed Spark DataFrame into the Delta table, enabling schema merging.

    Args:
        df (DataFrame): The transformed Spark DataFrame.
        table_name (str): The name of the Delta table to update.
        database (str): The Databricks Catalog database containing the table.

    Returns:
        None
    """
    df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{database}.{table_name}")
    
    print(f"Transformed data successfully loaded into table: {database}.{table_name}")
