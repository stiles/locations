"""
Kung Fu Tea locations scraper

Scrapes store locations from Kung Fu Tea's StorePoint API endpoint.
"""

import pandas as pd
import requests
from src.core.base_scraper import BaseScraper


class KungFuTeaScraper(BaseScraper):
    """Scraper for Kung Fu Tea locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Kung Fu Tea locations from their StorePoint API
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        url = "https://api.storepoint.co/v1/15dcedfc240d49/locations?rq"
        
        response = self.session.get(url, timeout=self.config.get('timeout', 30))
        response.raise_for_status()
        
        data = response.json()
        
        if "results" not in data or "locations" not in data["results"]:
            raise ValueError("Unexpected API response structure")
        
        locations = data["results"]["locations"]
        
        if not locations:
            raise ValueError("No locations found")
        
        # Extract data from each location
        store_list = []
        for location in locations:
            store_dict = {
                "store_id": location.get("id"),
                "address": location.get("streetaddress"),
                "latitude": location.get("loc_lat"),
                "longitude": location.get("loc_long"),
                "phone": location.get("phone"),
                "name": location.get("name", "Kung Fu Tea"),
                "city": location.get("city"),
                "state": location.get("state"),
                "zip_code": location.get("postal_code")
            }
            store_list.append(store_dict)
        
        df = pd.DataFrame(store_list)
        return df
