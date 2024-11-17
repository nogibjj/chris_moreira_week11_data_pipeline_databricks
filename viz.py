"""
Generate a bar chart to visualize top artists by total streams.
"""

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession


def create_spark_session(app_name="Databricks Visualization"):
    """
    Creates a Spark session, using Databricks shared SparkSession if available.
    """
    if SparkSession.getActiveSession():
        print("Detected Databricks runtime. Using shared SparkSession.")
        return SparkSession.getActiveSession()
    else:
        print("Running in local mode.")
        return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()


def visualize_top_artists(data):
    """
    Creates a bar chart showing the top 5 artists by total streams.
    """
    data = data.sort_values("total_streams", ascending=False)
    plt.figure(figsize=(10, 6))

    bars = plt.bar(data["artist_name"], data["total_streams"], color="skyblue")

    for bar, danceability, energy in zip(
        bars, data["avg_danceability"], data["avg_energy"]
    ):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"D:{danceability}, E:{energy}",
            ha="center",
            va="bottom",
            fontsize=9,
        )

    plt.title("Top 5 Artists by Total Streams", fontsize=14)
    plt.xlabel("Artist Name", fontsize=12)
    plt.ylabel("Total Streams (Billions)", fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def main():
    """
    Executes the SQL query and generates the visualization.
    """
    spark = create_spark_session()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table"

    query = f"""
    SELECT 
        artists_name AS artist_name, 
        SUM(CAST(streams AS BIGINT)) AS total_streams,
        ROUND(AVG(`danceability_%`), 2) AS avg_danceability,
        ROUND(AVG(`energy_%`), 2) AS avg_energy
    FROM {database}.{table_name}
    GROUP BY artists_name
    ORDER BY total_streams DESC
    LIMIT 5
    """

    data = spark.sql(query).toPandas()
    visualize_top_artists(data)


if __name__ == "__main__":
    main()
