def extract(url, table_name, database="csm_87_database"):
    """
    Extract data from URL, clean, and save to Delta table.

    Args:
        url (str): URL for the CSV file to download.
        table_name (str): Name of the Delta table to create.
        database (str): Databricks Catalog database.

    Returns:
        None
    """
    spark = SparkSession.builder.getOrCreate()

    # Load CSV into Pandas DataFrame
    df = pd.read_csv(url)

    # Clean column names
    df.columns = [
        col.strip().replace(" ", "_").replace("(", "")
        .replace(")", "")
        for col in df.columns
    ]

    # Convert to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Save DataFrame as Delta table
    try:
        spark_df.write.format("delta").mode("overwrite").saveAsTable(
            f"{database}.{table_name}"
        )
    except Exception as e:
        print(f"Failed to save Delta table: {e}")
        raise

    # Check if table exists
    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        raise RuntimeError(f"Table {table_name} not found in {database}.")
