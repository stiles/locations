"""
Wendy's locations scraper

Scrapes store locations from Wendy's API using strategically placed ZIP codes with large radius.
"""

import pandas as pd
import requests
import time
import logging
from src.core.base_scraper import BaseScraper


class WendysScraper(BaseScraper):
    """Scraper for Wendy's locations"""

    def __init__(self, company_name: str, config: dict):
        super().__init__(company_name, config)
        self.logger = logging.getLogger(__name__)

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Wendy's locations using comprehensive ZIP code iteration
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        # Load ZIP codes from reference file for comprehensive coverage
        import pandas as pd
        zips_df = pd.read_csv("_reference/data/zips_reference.csv")
        zip_codes = zips_df['zip'].astype(str).str.zfill(5).tolist()
        
        self.logger.info(f"Starting Wendy's scrape with {len(zip_codes)} ZIP codes")
        
        headers = {
            "authority": "api.app.prd.wendys.digital",
            "accept": "application/json",
            "content-type": "application/json",
            "origin": "https://order.wendys.com",
            "referer": "https://order.wendys.com/",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        }
        
        # Update session headers
        self.session.headers.update(headers)
        
        all_stores = []
        seen_names = set()  # Track unique stores to avoid duplicates (using name as identifier)
        
        for i, zip_code in enumerate(zip_codes):
            try:
                url = (f"https://api.app.prd.wendys.digital/web-client-gateway/LocationServices/rest/nearbyLocations"
                      f"?&lang=en&cntry=US&sourceCode=ORDER.WENDYS&version=23.0.1&address={zip_code}&limit=100&radius=50")
                
                response = self.session.get(url, timeout=self.config.get('timeout', 30))
                response.raise_for_status()
                
                data = response.json()
                
                if "data" in data:
                    stores = data["data"]
                    
                    for store in stores:
                        store_name = store.get("name")
                        
                        # Skip if we've already seen this store (use name for deduplication)
                        if store_name in seen_names:
                            continue
                            
                        seen_names.add(store_name)
                        
                        # Extract address information
                        store_dict = {
                            "name": store_name,
                            "address": store.get("address"),
                            "city": store.get("city"),
                            "state": store.get("state"),
                            "zip_code": store.get("zipCode"),
                            "phone": store.get("phoneNumber"),
                            "latitude": store.get("latitude"),
                            "longitude": store.get("longitude"),
                            "store_id": store.get("id"),
                        }
                        all_stores.append(store_dict)
                
                if i % 100 == 0:
                    self.logger.info(f"Processed {i+1}/{len(zip_codes)} ZIP codes, found {len(all_stores)} total locations")
                
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
