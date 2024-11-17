from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

def main():
    """
    Query and visualize data.
    """
    spark = SparkSession.builder.getOrCreate()
    database = "csm_87_database"
    table_name = "csm_87_Spotify_Table_transformed"

    if not spark.catalog.tableExists(f"{database}.{table_name}"):
        raise ValueError(f"Table {table_name} not found in {database}.")

    result = spark.sql(
        f"SELECT Artist, COUNT(*) AS SongCount "
        f"FROM {database}.{table_name} "
        "GROUP BY Artist ORDER BY SongCount DESC LIMIT 10"
    )

    # Convert result to Pandas for visualization
    pdf = result.toPandas()
    pdf.plot.bar(x="Artist", y="SongCount")
    plt.title("Top 10 Artists by Song Count")
    plt.savefig("output_viz.png")
