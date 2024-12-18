import unittest
from unittest.mock import patch
from src import app

class TestClass(unittest.TestCase):
    
    ### Test Next Hour Function ###
    
    def test_next_hour_simple_case(self):
        expected = "2024-11-01T01:00:00.000Z"
        actual, _ = app.round_to_next_hour_range("2024-11-01T00:00:00.000Z")
        self.assertEqual(expected, actual)	
        
    def test_next_hour_using_23hour(self):
        expected = "2024-11-02T00:00:00.000Z"
        actual, _ = app.round_to_next_hour_range("2024-11-01T23:59:16.000Z")
        self.assertEqual(expected, actual)	
        
    def test_next_hour_using_previous_month(self):
        expected = "2024-11-01T00:00:00.000Z"
        actual, _ = app.round_to_next_hour_range("2024-10-31T23:59:16.000Z")
        self.assertEqual(expected, actual)	
      
    def test_next_hour_date_end(self):
        expected = "2024-11-01T01:00:00.000Z"
        _, actual = app.round_to_next_hour_range("2024-10-31T23:59:16.000Z")
        self.assertEqual(expected, actual)	  
        
        
    ### Test Next Day Function ###
        
    def test_next_day_simple_case(self):
        expected = "2024-11-02T00:00:00.000Z"
        actual, _ = app.round_to_next_day_range("2024-11-01T00:00:00.000Z")
        self.assertEqual(expected, actual)	
        
    def test_next_day_using_previous_month(self):
        expected = "2024-11-01T00:00:00.000Z"
        actual, _ = app.round_to_next_day_range("2024-10-31T23:59:16.000Z")
        self.assertEqual(expected, actual)	    

    def test_next_day_date_end(self):
        expected = "2024-11-02T00:00:00.000Z"
        _, actual = app.round_to_next_day_range("2024-10-31T23:59:16.000Z")
        self.assertEqual(expected, actual)	
        
        
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



if __name__=='__main__':
    unittest.main()