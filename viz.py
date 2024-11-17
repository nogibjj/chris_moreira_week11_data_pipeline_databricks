import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession

def create_spark_session():
    """
    Create and return a Spark session.
    """
    return SparkSession.builder.getOrCreate()

def main():
    """
    Query and visualize data from the Delta table.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table_transformed"

    # Check if table exists
    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        print(f"Table {table_name} not found in {database}.")
        print("Run the extract step to create the table.")
        return

    # Query the data
    query = f"""
    SELECT Artist, SUM(Stream_Count) AS Total_Streams
    FROM {database}.{table_name}
    GROUP BY Artist
    ORDER BY Total_Streams DESC
    LIMIT 10
    """
    result = spark.sql(query).toPandas()

    # Handle empty results
    if result.empty:
        print("No data available for visualization.")
        return

    # Plot the data
    plt.figure(figsize=(10, 6))
    plt.bar(result['Artist'], result['Total_Streams'], color='blue')
    plt.xlabel("Artist")
    plt.ylabel("Total Streams")
    plt.title("Top 10 Artists by Stream Count")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    # Save and display the plot
    plot_path = "top_artists_streams.png"
    plt.savefig(plot_path)
    plt.show()
    print(f"Visualization saved to {plot_path}.")
