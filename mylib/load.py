"""
Load data into Delta table.
"""

def load_data(df, table_name, database):
    """
    Load transformed data to Delta.
    """
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{database}.{table_name}"
    )
    print(f"Data loaded: {database}.{table_name}")
