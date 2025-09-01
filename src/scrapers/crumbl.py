"""
Crumbl Cookies locations scraper

Scrapes store locations from Crumbl's Next.js data API endpoint.
"""

import pandas as pd
import requests
from src.core.base_scraper import BaseScraper


class CrumblScraper(BaseScraper):
    """Scraper for Crumbl Cookies locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Crumbl locations from their Next.js data API
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        # This endpoint returns all active stores in a Next.js data format
        url = "https://crumblcookies.com/_next/data/V67rG5TouMkXfSOJ11uDw/en-US/stores.json"
        
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        
        response = self.session.get(url, headers=headers, timeout=self.config.get('timeout', 30))
        response.raise_for_status()
        
        data = response.json()
        
        if "pageProps" not in data or "allActiveStores" not in data["pageProps"]:
            raise ValueError("Unexpected API response structure")
        
        stores = data["pageProps"]["allActiveStores"]
        
        if not stores:
            raise ValueError("No stores found")
        
        # Extract data from each store
        store_dicts = []
        for store in stores:
            store_dict = {
                "store_id": store.get("slug"),
                "name": store.get("name"),
                "address": store.get("street"),  # Map 'street' to 'address'
                "city": store.get("city"),
                "state": store.get("state"),
                "zip_code": store.get("zip"),
                "email": store.get("email"),
                "phone": store.get("phone"),
                "latitude": store.get("latitude"),
                "longitude": store.get("longitude"),
            }
            store_dicts.append(store_dict)
        
        df = pd.DataFrame(store_dicts)
        return df
