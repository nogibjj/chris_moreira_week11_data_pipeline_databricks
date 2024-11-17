from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

def run_viz():
    spark = SparkSession.builder \
        .appName("Spotify_Viz") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.1") \
        .getOrCreate()

    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table"

    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        raise ValueError(f"Table {table_name} not found.")

    query = f"""
        SELECT artist_name, AVG(danceability) AS avg_danceability
        FROM {database}.{table_name}
        GROUP BY artist_name
        ORDER BY avg_danceability DESC
        LIMIT 5
    """
    result = spark.sql(query).toPandas()

    # Plot the results
    plt.bar(result['artist_name'], result['avg_danceability'])
    plt.xlabel('Artist Name')
    plt.ylabel('Average Danceability')
    plt.title('Top 5 Artists by Danceability')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
