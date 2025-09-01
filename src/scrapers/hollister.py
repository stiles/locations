"""
Hollister locations scraper

Scrapes store locations from Hollister's API using ZIP code iteration.
"""

import pandas as pd
import requests
import time
from src.core.base_scraper import BaseScraper


class HollisterScraper(BaseScraper):
    """Scraper for Hollister locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Hollister locations using ZIP code iteration
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        # Load ZIP codes for iteration
        zips_df = pd.read_json("_reference/data/zips_reference_pop_gen.json")
        zips_df = zips_df.sort_values("population", ascending=False)
        
        # Use top 500 most populous ZIP codes to get good coverage
        zips_top = zips_df.head(500)
        zips = zips_top['zip'].to_list()
        
        headers = {
            "authority": "www.abercrombie.com",
            "accept": "application/json, text/javascript, */*; q=0.01",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        
        all_stores = []
        unique_stores = set()  # Track unique stores to avoid duplicates
        
        for zip_code in zips:
            try:
                # Get lat/lng for this ZIP code
                zip_info = zips_df[zips_df['zip'] == zip_code].iloc[0]
                lat = round(zip_info['lat'], 5)
                lng = round(zip_info['lng'], 5)
                
                params = {
                    "country": "US",
                    "latitude": str(lat),
                    "longitude": str(lng),
                    "radius": "200",
                    "radiusUOM": "SMI",
                }
                
                response = self.session.get(
                    "https://www.hollisterco.com/api/ecomm/h-us/storelocator/search",
                    params=params,
                    headers=headers,
                    timeout=self.config.get('timeout', 30)
                )
                response.raise_for_status()
                
                data = response.json()
                
                if "physicalStores" in data:
                    stores = data["physicalStores"]
                    
                    for store in stores:
                        store_number = store.get("storeNumber")
                        
                        # Skip if we've already seen this store
                        if store_number in unique_stores:
                            continue
                            
                        unique_stores.add(store_number)
                        
                        # Extract address (first line)
                        address_lines = store.get("addressLine", [])
                        address = address_lines[0] if address_lines else ""
                        
                        store_dict = {
                            "store_id": store_number,
                            "name": store.get("name"),
                            "address": address,
                            "city": store.get("city"),
                            "state": store.get("stateOrProvinceName"),
                            "zip_code": store.get("postalCode"),
                            "phone": store.get("telephone"),
                            "latitude": store.get("latitude"),
                            "longitude": store.get("longitude"),
                        }
                        all_stores.append(store_dict)
                
                # Rate limiting - be respectful
                time.sleep(0.5)
                
            except Exception as e:
                # Continue with next ZIP code if this one fails
                continue
        
        if not all_stores:
            raise ValueError("No stores found")
        
        df = pd.DataFrame(all_stores)
        return df
