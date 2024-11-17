def single_query_main():
    """
    Execute SQL query on the Delta table.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table_transformed"

    # Check if table exists
    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        print(f"Table {table_name} not found in {database}.")
        print("Ensure the extract step ran successfully.")
        return

    # Execute SQL query
    query = f"SELECT * FROM {database}.{table_name} LIMIT 10"
    result = spark.sql(query)
    result.show()
