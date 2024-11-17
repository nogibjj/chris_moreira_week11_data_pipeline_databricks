from main import main

def test_main():
    """
    Test the main ETL process.
    """
    try:
        main()
    except Exception as e:
        raise AssertionError(f"main.py failed: {str(e)}")
