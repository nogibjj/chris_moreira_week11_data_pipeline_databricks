from main import main as run_main
from query_run import single_query_main as run_query
from viz import main as run_viz

def test_main():
    """Test main.py execution."""
    try:
        print("Testing main.py...")
        run_main()
        print("main.py ran successfully.")
    except Exception as e:
        raise AssertionError(f"main.py failed: {e}")

def test_query():
    """Test query_run.py execution."""
    try:
        print("Testing query_run.py...")
        run_query()
        print("query_run.py ran successfully.")
    except Exception as e:
        raise AssertionError(f"query_run.py failed: {e}")

def test_viz():
    """Test viz.py execution."""
    try:
        print("Testing viz.py...")
        run_viz()
        print("viz.py ran successfully.")
    except Exception as e:
        raise AssertionError(f"viz.py failed: {e}")

if __name__ == "__main__":
    print("Starting tests...")
    try:
        test_main()
        test_query()
        test_viz()
        print("All tests passed.")
    except AssertionError as error:
        print(error)
