from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

def create_spark_session():
    """
    Create and return a Spark session.
    """
    return SparkSession.builder.getOrCreate()

def main():
    """
    Query and visualize data.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table_transformed"

    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        raise ValueError(f"Table {table_name} not found in {database}.")

    query = f"""
    SELECT Artist, SUM(Stream_Count) AS Total_Streams
    FROM {database}.{table_name}
    GROUP BY Artist
    ORDER BY Total_Streams DESC
    LIMIT 10
    """
    data = spark.sql(query).toPandas()

    plt.bar(data["Artist"], data["Total_Streams"])
    plt.xlabel("Artist")
    plt.ylabel("Total Streams")
    plt.title("Top 10 Artists by Total Streams")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig("top_artists.png")
    print("Visualization saved as 'top_artists.png'")
