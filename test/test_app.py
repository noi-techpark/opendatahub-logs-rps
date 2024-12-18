import unittest
from unittest.mock import patch
from src import app

class TestClass(unittest.TestCase):
        
    ### Test Set Start Date Function ###
    
    @patch('app.request.get_latest_record')
    def test_set_start_date_with_date_input_and_no_latest_date(self, mock_get_latest_record):
        mock_get_latest_record.return_value = None
        result = app.set_start_date("2024-11-01T00:00:00.000Z", "test_storage_index", "test_client")
        self.assertEqual(result, "2024-11-01T00:00:00.000Z")
        mock_get_latest_record.assert_called_once_with("test_client", "test_storage_index")
        
    @patch('app.request.get_latest_record')
    def test_set_start_date_without_date_input_and_with_latest_date(self, mock_get_latest_record):
        mock_get_latest_record.return_value = "2024-12-11T23:59:58.000Z"
        result = app.set_start_date("", "test_storage_index", "test_client")
        self.assertEqual(result, "2024-12-12T00:00:00.000Z")
        mock_get_latest_record.assert_called_once_with("test_client", "test_storage_index")
        
    @patch('app.request.get_latest_record')
    def test_set_start_date_with_date_input_and_with_latest_date(self, mock_get_latest_record):
        mock_get_latest_record.return_value = "2024-12-11T23:59:58.000Z"
        result = app.set_start_date("2024-11-01T00:00:00.000Z", "test_storage_index", "test_client")
        self.assertEqual(result, "2024-12-12T00:00:00.000Z")
        mock_get_latest_record.assert_called_once_with("test_client", "test_storage_index")
        
    @patch('app.request.get_latest_record')
    def test_set_start_date_with_date_input_larger_than_latest_date(self, mock_get_latest_record):
        mock_get_latest_record.return_value = "2024-12-11T23:59:58.000Z"
        result = app.set_start_date("2024-12-31T00:00:00.000Z", "test_storage_index", "test_client")
        self.assertEqual(result, "2024-12-31T00:00:00.000Z")
        mock_get_latest_record.assert_called_once_with("test_client", "test_storage_index")


    ### Test Validate Date Function ###
    
    def test_invalid_date(self):
        date = "2024-11-01T01:00:00.000"
        self.assertRaises(Exception, app.validate_date, date)	
    
    def test_valid_date(self):
        date = "2024-11-01T01:00:00.000Z"
        self.assertIsNone(app.validate_date(date))


if __name__=='__main__':
    unittest.main()