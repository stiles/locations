import pytest
import pandas as pd
from unittest.mock import Mock, patch
from src.core.base_scraper import BaseScraper
from src.core.data_processor import DataProcessor
from src.core.storage import S3Storage


class TestBaseScraper:
    """Test the BaseScraper abstract class"""
    
    def test_base_scraper_initialization(self):
        """Test that BaseScraper initializes correctly"""
        
        class TestScraper(BaseScraper):
            def scrape(self):
                return pd.DataFrame([
                    {"name": "Test Store", "address": "123 Main St", "city": "Test City", "state": "CA"}
                ])
        
        config = {"timeout": 30, "headers": {"User-Agent": "test"}}
        scraper = TestScraper("test-company", config)
        
        assert scraper.company_name == "test-company"
        assert scraper.config == config
        assert scraper.session.headers["User-Agent"] == "test"
    
    def test_validate_data_success(self):
        """Test data validation with valid data"""
        
        class TestScraper(BaseScraper):
            def scrape(self):
                return pd.DataFrame()
        
        scraper = TestScraper("test", {})
        data = pd.DataFrame([
            {"name": "Store", "address": "123 St", "city": "City", "state": "CA"}
        ])
        
        # Should not raise an exception
        scraper.validate_data(data)
    
    def test_validate_data_missing_columns(self):
        """Test data validation with missing required columns"""
        
        class TestScraper(BaseScraper):
            def scrape(self):
                return pd.DataFrame()
        
        scraper = TestScraper("test", {})
        data = pd.DataFrame([{"name": "Store"}])  # Missing required columns
        
        with pytest.raises(ValueError, match="Missing required columns"):
            scraper.validate_data(data)
    
    def test_validate_data_empty(self):
        """Test data validation with empty dataframe"""
        
        class TestScraper(BaseScraper):
            def scrape(self):
                return pd.DataFrame()
        
        scraper = TestScraper("test", {})
        data = pd.DataFrame()
        
        with pytest.raises(ValueError, match="No locations found"):
            scraper.validate_data(data)


class TestDataProcessor:
    """Test the DataProcessor class"""
    
    def test_clean_address(self):
        """Test address cleaning functionality"""
        processor = DataProcessor()
        
        # Test normal case
        result = processor.clean_address("  123   Main   St  ")
        assert result == "123 Main St"
        
        # Test NaN case
        result = processor.clean_address(pd.NA)
        assert result == ""
    
    def test_standardize_columns(self):
        """Test column standardization"""
        processor = DataProcessor()
        
        df = pd.DataFrame([
            {"name": "Store", "address": "123 St", "city": "City", "state": "ca"}
        ])
        
        result = processor.standardize_columns(df, "test-company")
        
        assert "company" in result.columns
        assert "scraped_date" in result.columns  
        assert "location_id" in result.columns
        assert result["company"].iloc[0] == "test-company"
        assert result["state"].iloc[0] == "CA"  # Should be uppercase
    
    def test_get_quality_metrics(self):
        """Test data quality metrics calculation"""
        processor = DataProcessor()
        
        df = pd.DataFrame([
            {"name": "Store1", "address": "123 St", "city": "City", "state": "CA", "latitude": 34.0, "longitude": -118.0},
            {"name": "Store2", "address": None, "city": "City", "state": "CA", "latitude": None, "longitude": None},
        ])
        
        metrics = processor.get_quality_metrics(df)
        
        assert metrics["total_locations"] == 2
        assert metrics["geocoded_locations"] == 1
        assert metrics["geocoding_rate"] == 0.5
        assert metrics["missing_addresses"] == 1
        assert metrics["states_covered"] == 1


class TestS3Storage:
    """Test the S3Storage class"""
    
    def test_local_storage_fallback(self):
        """Test that storage falls back to local when S3 is not configured"""
        # Test with no bucket configured
        storage = S3Storage(bucket_name=None)
        assert storage.use_local == True
    
    @patch('boto3.Session')
    def test_s3_storage_initialization(self, mock_session):
        """Test S3 storage initialization with proper config"""
        mock_session.return_value.client.return_value = Mock()
        
        storage = S3Storage(bucket_name="test-bucket")
        assert storage.bucket_name == "test-bucket"
        assert storage.s3_prefix == "locations/"
    
    def test_calculate_quality_metrics(self):
        """Test quality metrics calculation"""
        storage = S3Storage()
        
        df = pd.DataFrame([
            {"name": "Store", "address": "123 St", "city": "City", "state": "CA", "latitude": 34.0, "longitude": -118.0}
        ])
        
        metrics = storage._calculate_quality_metrics(df)
        
        assert metrics["total_locations"] == 1
        assert metrics["geocoded_locations"] == 1
        assert metrics["geocoding_rate"] == 1.0
        assert isinstance(metrics["total_locations"], int)  # Ensure proper JSON serialization


if __name__ == "__main__":
    pytest.main([__file__]) 