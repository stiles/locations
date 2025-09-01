"""
Pollo Tropical locations scraper

Scrapes store locations from Pollo Tropical's API endpoint for Florida locations.
"""

import pandas as pd
import requests
from src.core.base_scraper import BaseScraper


class PolloTropicalScraper(BaseScraper):
    """Scraper for Pollo Tropical locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Pollo Tropical locations from their Florida API endpoint
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        # Pollo Tropical is primarily a Florida chain
        url = "https://olo.pollotropical.com/api/vendors/search/FL"
        
        headers = {
            "accept": "application/json, */*",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "x-olo-request": "1",
        }
        
        response = self.session.get(url, headers=headers, timeout=self.config.get('timeout', 30))
        response.raise_for_status()
        
        data = response.json()
        
        if "vendor-search-results" not in data:
            raise ValueError("Unexpected API response structure")
        
        locations = data["vendor-search-results"]
        
        if not locations:
            raise ValueError("No locations found")
        
        # Create DataFrame with selected columns
        df = pd.DataFrame(locations)
        
        # Select relevant columns (the API provides both flat and nested address info)
        column_selection = ["id", "name", "slug", "phoneNumber", "streetAddress", "city", "state", "address", "latitude", "longitude"]
        df = df[column_selection]
        
        # Extract zip code from nested address object
        if "address" in df.columns:
            try:
                # Extract postal code from nested address
                df["zip_code"] = df["address"].apply(lambda x: x.get("postalCode", "") if isinstance(x, dict) else "")
                # Drop the nested address column
                df = df.drop(columns=["address"])
            except Exception:
                # If extraction fails, just drop the nested column
                df = df.drop(columns=["address"])
        
        # Rename columns to standard format
        df = df.rename(columns={
            "id": "store_id",
            "phoneNumber": "phone",
            "streetAddress": "address"
        })
        
        return df
