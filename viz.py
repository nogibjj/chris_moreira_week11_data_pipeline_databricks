"""
Generate bar chart for artists by streams.
"""

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

def create_spark_session(app_name="Spotify_Viz"):
    """
    Create or use Spark session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def visualize_top_artists(data):
    """
    Bar chart for top 5 artists by streams.
    """
    data = data.sort_values("total_streams", ascending=False)
    plt.figure(figsize=(10, 6))
    bars = plt.bar(data["artist_name"], data["total_streams"], color="skyblue")
    for bar, danceability, energy in zip(
        bars, data["avg_danceability"], data["avg_energy"]
    ):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                 f"D:{danceability}, E:{energy}", ha="center", fontsize=9)
    plt.title("Top 5 Artists by Streams")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def main():
    """
    Query and visualize data.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table_transformed"

    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        raise ValueError(f"Table {table_name} not found.")

    query = f"""
    SELECT artists_name AS artist_name, 
           SUM(CAST(streams AS BIGINT)) AS total_streams,
           ROUND(AVG(danceability_%), 2) AS avg_danceability,
           ROUND(AVG(energy_%), 2) AS avg_energy
    FROM {database}.{table_name}
    GROUP BY artists_name
    ORDER BY total_streams DESC
    LIMIT 5
    """
    data = spark.sql(query).toPandas()
    visualize_top_artists(data)

if __name__ == "__main__":
    main()
