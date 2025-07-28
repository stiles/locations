from abc import ABC, abstractmethod
import pandas as pd
import geopandas as gpd
from datetime import datetime
from typing import Dict, Optional, List
import requests
import logging
import os

# Initialize logger
logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    """Abstract base class for all location scrapers"""
    
    def __init__(self, company_name: str, config: dict):
        self.company_name = company_name
        self.config = config
        self.timestamp = datetime.now().strftime("%Y-%m-%d")
        self.session = requests.Session()
        self._setup_session()
        
    def _setup_session(self):
        """Configure requests session with headers and rate limiting"""
        self.session.headers.update(self.config.get('headers', {}))
        
    @abstractmethod
    def scrape(self) -> pd.DataFrame:
        """Implement company-specific scraping logic
        
        Returns:
            pd.DataFrame: Raw location data with location information.
            Column names will be standardized by DataProcessor.
            
            Common column variations that will be standardized:
                - store_id: storenumber, storeId, id
                - address: streetaddress, address1, street_address  
                - zip_code: zipcode, zip, postal_code
                - phone: phone_number, telephone
                - latitude/longitude: lat/lng, lat/lon
                
            Minimum required information (any column name variation):
                - name: Location name
                - address: Street address
                - city: City name  
                - state: State abbreviation
        """
        pass
    
    def validate_data(self, data: pd.DataFrame) -> None:
        """Validate scraped data has minimum required information
        Note: Column names will be standardized after this validation"""
        if len(data) == 0:
            raise ValueError("No locations found")
            
        # Check for essential location data (flexible column names)
        name_cols = ['name', 'store_name', 'location_name']
        address_cols = ['address', 'streetaddress', 'address1', 'street_address']  
        city_cols = ['city']
        state_cols = ['state']
        
        has_name = any(col in data.columns for col in name_cols)
        has_address = any(col in data.columns for col in address_cols)
        has_city = any(col in data.columns for col in city_cols)
        has_state = any(col in data.columns for col in state_cols)
        
        missing = []
        if not has_name: missing.append("name/store_name")
        if not has_address: missing.append("address/streetaddress")  
        if not has_city: missing.append("city")
        if not has_state: missing.append("state")
        
        if missing:
            raise ValueError(f"Missing essential location data: {missing}")
            
    def run(self) -> Dict:
        """Standard workflow: scrape → validate → process → store → return summary"""
        try:
            # Import here to avoid circular imports
            from .data_processor import DataProcessor
            from .storage import S3Storage
            
            # Initialize components
            processor = DataProcessor()
            storage = S3Storage()
            
            # Scrape raw data
            logger.info(f"Starting scrape for {self.company_name}")
            raw_data = self.scrape()
            
            # Validate data
            self.validate_data(raw_data)
            
            # Store raw data for debugging
            storage.save_raw_data(raw_data, self.company_name, self.timestamp)
            
            # Process data (clean, geocode, standardize)
            processed_data = processor.process(raw_data, self.company_name)
            
            # Store processed data to S3
            urls = storage.save_processed_data(
                processed_data, self.company_name, self.timestamp
            )
            
            # Return summary
            return {
                "company": self.company_name,
                "locations_count": len(processed_data),
                "timestamp": self.timestamp,
                "files": urls,
                "status": "success",
                "data_quality": processor.get_quality_metrics(processed_data)
            }
            
        except Exception as e:
            logger.error(f"Error scraping {self.company_name}: {str(e)}")
            return {
                "company": self.company_name,
                "status": "error",
                "error": str(e),
                "timestamp": self.timestamp
            } 