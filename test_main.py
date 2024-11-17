"""
Tests for main.py
"""

from main import main as run_main
from query_run import single_query_main as run_query
from viz import main as run_viz

def test_main():
    """
    Test main.py execution.
    """
    try:
        run_main()
    except Exception as e:
        raise AssertionError(f"main.py failed: {e}")

def test_query():
    """
    Test query_run.py execution.
    """
    try:
        run_query()
    except Exception as e:
        raise AssertionError(f"query_run.py failed: {e}")

def test_viz():
    """
    Test viz.py execution.
    """
    try:
        run_viz()
    except Exception as e:
        raise AssertionError(f"viz.py failed: {e}")
