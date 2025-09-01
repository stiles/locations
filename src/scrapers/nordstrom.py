"""
Nordstrom locations scraper

Scrapes store locations from Nordstrom's API using ZIP code iteration.
"""

import pandas as pd
import requests
import time
from src.core.base_scraper import BaseScraper


class NordstromScraper(BaseScraper):
    """Scraper for Nordstrom locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Nordstrom locations using ZIP code iteration
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        # Load ZIP codes for iteration
        zips_df = pd.read_json("_reference/data/zips_reference_pop_gen.json")
        zips_df = zips_df.sort_values("population", ascending=False)
        
        # Use top 1000 most populous + sample of 1000 others for good coverage
        zips_top = zips_df.head(1000)
        zips_sample = zips_df.tail(len(zips_df) - 1000).sample(1000, random_state=42)
        zips_combined = pd.concat([zips_sample, zips_top]).reset_index(drop=True)
        zips_combined = zips_combined.sort_values("population", ascending=False)
        
        zips = zips_combined["zip"].astype(str).str.zfill(5).to_list()
        
        headers = {
            "authority": "api.nordstrom.com",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        
        # Update session headers
        self.session.headers.update(headers)
        
        all_stores = []
        seen_store_numbers = set()  # Track unique stores to avoid duplicates
        
        for zip_code in zips:
            try:
                params = {
                    "distance": "50",
                    "sortOrder": "asc",
                    "apikey": "Gneq2B6KqSbEABkg9IDRxuxAef9BqusJ",  # Public API key from notebook
                }
                
                response = self.session.get(
                    f"https://api.nordstrom.com/v2/store/postalcode/{zip_code}",
                    params=params,
                    timeout=self.config.get('timeout', 30)
                )
                response.raise_for_status()
                
                data = response.json()
                
                if "stores" in data:
                    stores = data["stores"]
                    
                    for store in stores:
                        store_number = store.get("number")
                        
                        # Skip if we've already seen this store
                        if store_number in seen_store_numbers:
                            continue
                            
                        seen_store_numbers.add(store_number)
                        
                        store_dict = {
                            "store_id": store_number,
                            "store_type": store.get("type"),
                            "name": store.get("name"),
                            "address": store.get("address"),
                            "city": store.get("city"),
                            "state": store.get("state"),
                            "zip_code": store.get("zipCode"),
                            "phone": store.get("phone"),
                            "latitude": store.get("latitude"),
                            "longitude": store.get("longitude"),
                            "url": store.get("rackStorePath"),
                            "timezone": store.get("timeZone"),
                        }
                        all_stores.append(store_dict)
                
                # Rate limiting - be respectful
                time.sleep(0.1)
                
            except requests.HTTPError as e:
                # Log HTTP errors but continue with next ZIP
                self.logger.warning(f"HTTP Error for ZIP code {zip_code}: {e}")
                continue
            except Exception as e:
                # Log other errors but continue with next ZIP
                self.logger.warning(f"Error for ZIP code {zip_code}: {e}")
                continue
        
        if not all_stores:
            raise ValueError("No stores found")
        
        df = pd.DataFrame(all_stores)
        return df
