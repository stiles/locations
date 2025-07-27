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
            pd.DataFrame: Raw location data with at minimum:
                - name: Location name
                - address: Full address
                - city: City name
                - state: State abbreviation
                - zip_code: ZIP code
                - latitude: Latitude (if available)
                - longitude: Longitude (if available)
        """
        pass
    
    def validate_data(self, data: pd.DataFrame) -> None:
        """Validate scraped data meets minimum requirements"""
        required_columns = ['name', 'address', 'city', 'state']
        missing_columns = set(required_columns) - set(data.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        if len(data) == 0:
            raise ValueError("No locations found")
            
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