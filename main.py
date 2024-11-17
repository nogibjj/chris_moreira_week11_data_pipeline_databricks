from mylib.extract import extract
from mylib.transform import transform_data
from mylib.load import load_data


def main():
    """
    Orchestrates the ETL pipeline.
    """
    database = "csm_87_database"
    raw_table = "csm_87_Spotify_Table"
    transformed_table = "csm_87_Spotify_Table_transformed"
    url = (
        "https://raw.githubusercontent.com/nogibjj/"
        "chris_moreira_week6_sql_databricks/main/data/"
        "Spotify_Most_Streamed_Songs.csv"
    )

    try:
        print("Extracting data...")
        extract(url, raw_table, database)

        print("Transforming data...")
        transform_data(database, raw_table, transformed_table)

        print("Loading data...")
        load_data(database, transformed_table)

    except Exception as e:
        print(f"ETL pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()
