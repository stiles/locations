"""
99 Ranch Market locations scraper

Scrapes store locations from 99 Ranch Market's API endpoint.
"""

import pandas as pd
import requests
from src.core.base_scraper import BaseScraper


class Ranch99Scraper(BaseScraper):
    """Scraper for 99 Ranch Market locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape 99 Ranch Market locations from their API
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "time-zone": "America/Los_Angeles",
        }

        # This API returns all stores when we search with a high page size
        json_data = {
            "zipCode": "90066",  # Any ZIP code works, API returns all stores
            "pageSize": 100,     # High page size to get all stores
            "pageNum": 1,
            "type": 1,
            "source": "WEB",
            "within": None,
        }
        
        response = self.session.post(
            "https://api.awsprod.99ranch.com/store/web/nearby/stores",
            headers=headers,
            json=json_data,
            timeout=self.config.get('timeout', 30)
        )
        response.raise_for_status()
        
        data = response.json()
        
        if "data" not in data or "records" not in data["data"]:
            raise ValueError("Unexpected API response structure")
        
        stores = data["data"]["records"]
        
        if not stores:
            raise ValueError("No stores found")
        
        # Create DataFrame and select relevant columns
        df = pd.DataFrame(stores)
        
        # Select and rename columns to match our standard format
        selected_columns = [
            "storeNumber",
            "name", 
            "state",
            "city",
            "street",
            "zipCode",
            "longitude",
            "latitude",
            "timeZone",
        ]
        
        # Only select columns that exist in the response
        available_columns = [col for col in selected_columns if col in df.columns]
        df = df[available_columns]
        
        # Rename to standard format
        df = df.rename(columns={
            "storeNumber": "store_id",
            "street": "address",
            "zipCode": "zip_code"
        })
        
        return df
