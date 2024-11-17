from main import main
from unittest.mock import patch


def test_main():
    """
    Test main ETL pipeline execution.
    """
    with patch("main.extract") as mock_extract, \
         patch("main.transform_data") as mock_transform, \
         patch("main.load_data") as mock_load:
        main()
        mock_extract.assert_called_once()
        mock_transform.assert_called_once()
        mock_load.assert_called_once()
