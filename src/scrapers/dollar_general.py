"""
Dollar General locations scraper

Scrapes store locations from Dollar General's API using representative ZIP coordinates.
"""

import pandas as pd
import requests
import time
import logging
from src.core.base_scraper import BaseScraper


class DollarGeneralScraper(BaseScraper):
    """Scraper for Dollar General locations"""

    def __init__(self, company_name: str, config: dict):
        super().__init__(company_name, config)
        self.logger = logging.getLogger(__name__)

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Dollar General locations using coordinate-based API
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        # Load representative ZIP codes for efficient coverage
        # This gives us geographically dispersed coordinates without overlap
        zips_df = pd.read_json("_reference/representative_zip_codes.json")
        zips_df["zipcode"] = zips_df["zipcode"].astype(str).str.zfill(5)
        
        cookies = {
            "uniqueDeviceId": "10e4d653-fe3d-4916-afdc-29e15f9d1ed8",
            "gig_canary_ver": "15791-3-28506195",
            "omniSession": "OAJ70VLWNVGF6T4FK5238WUI1OGND5T4|000000000000000000000000000048533195|null|false|null",
        }

        headers = {
            "authority": "www.dollargeneral.com",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        
        # Update session
        self.session.headers.update(headers)
        self.session.cookies.update(cookies)
        
        all_stores = []
        seen_store_numbers = set()  # Track unique stores to avoid duplicates
        
        for index, row in zips_df.iterrows():
            try:
                zip_code = row["zipcode"]
                longitude = row["longitude"]
                latitude = row["latitude"]

                params = {
                    "latitude": latitude,
                    "longitude": longitude,
                    "radius": "100",  # 100 mile radius for good coverage
                    "pageSize": "1000",  # Large page size to get all stores
                }

                response = self.session.get(
                    "https://www.dollargeneral.com/bin/omni/pickup/storeSearchInventory",
                    params=params,
                    timeout=self.config.get('timeout', 30)
                )
                response.raise_for_status()
                
                data = response.json()
                
                if "stores" in data:
                    stores = data["stores"]
                    
                    for store in stores:
                        store_number = store.get("storeNumber")
                        
                        # Skip if we've already seen this store
                        if store_number in seen_store_numbers:
                            continue
                            
                        seen_store_numbers.add(store_number)
                        
                        store_dict = {
                            "store_id": store_number,
                            "address": store.get("address"),
                            "city": store.get("city"),
                            "state": store.get("state"),
                            "zip_code": store.get("zipCode"),
                            "latitude": store.get("latitude"),
                            "longitude": store.get("longitude"),
                            "phone": store.get("phoneNumber"),
                        }
                        all_stores.append(store_dict)
                
                # Rate limiting - be respectful
                time.sleep(0.1)
                
            except Exception as e:
                # Log errors but continue with next ZIP
                self.logger.warning(f"Error for ZIP code {zip_code}: {e}")
                continue
        
        if not all_stores:
            raise ValueError("No stores found")
        
        df = pd.DataFrame(all_stores)
        
        # Add name field since all stores are Dollar General
        df["name"] = "Dollar General"
        
        return df
