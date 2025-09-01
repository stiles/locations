"""
Meijer locations scraper

Scrapes store locations from Meijer's API using ZIP code iteration.
Meijer is a Midwest supermarket chain operating in MI, OH, IN, IL, KY, WI.
"""

import pandas as pd
import requests
import time
from src.core.base_scraper import BaseScraper


class MeijerScraper(BaseScraper):
    """Scraper for Meijer locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Meijer locations using ZIP code iteration for Midwest states
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        # Meijer operates in these Midwest states
        STATES = ['MI', 'OH', 'IN', 'IL', 'KY', 'WI']
        
        # Load ZIP codes for iteration - filter to Meijer's operating region
        zips_df = pd.read_json("_reference/data/zips_reference_pop_gen.json")
        zips_midwest = zips_df[zips_df["state"].isin(STATES)]
        
        # Sort by population for better coverage
        zips_midwest = zips_midwest.sort_values("population", ascending=False)
        zip_list = zips_midwest["zip"].astype(str).tolist()
        
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9,es;q=0.8',
            'cache-control': 'no-cache, no-store, must-revalidate, max-age=-1, private',
            'priority': 'u=1, i',
            'referer': 'https://www.meijer.com/shopping/store-finder.html',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'x-dtpc': '10$540415666_519h9vUKNAWKAATMRDBIRRUUQRIMMTDAILHCSJ-0e0',
        }
        
        # Update session headers
        self.session.headers.update(headers)
        
        all_stores = []
        seen_ids = set()  # Track unique stores to avoid duplicates
        
        for zip_code in zip_list:
            try:
                params = {
                    "locationQuery": zip_code,
                    "radius": "100"  # 100 mile radius
                }
                
                response = self.session.get(
                    "https://www.meijer.com/bin/meijer/store/search",
                    params=params,
                    timeout=self.config.get('timeout', 30)
                )
                response.raise_for_status()
                
                data = response.json()
                stores = data.get("pointsOfService", []) or []
                
                for store in stores:
                    store_id = store.get("mfcStoreId")
                    
                    # Skip if we've already seen this store or no ID
                    if not store_id or store_id in seen_ids:
                        continue
                        
                    seen_ids.add(store_id)
                    
                    # Extract address information
                    addr = store.get("address") or {}
                    region = (addr.get("region") or {})
                    gp = store.get("geoPoint") or {}
                    
                    store_dict = {
                        "store_id": store_id,
                        "name": store.get("displayName"),
                        "address": addr.get("line1"),
                        "city": addr.get("town"),
                        "state": region.get("isocode"),
                        "zip_code": addr.get("postalCode"),
                        "latitude": gp.get("latitude"),
                        "longitude": gp.get("longitude"),
                        "phone": store.get("phone"),
                    }
                    all_stores.append(store_dict)
                
                # Rate limiting - be respectful
                time.sleep(0.1)
                
            except requests.HTTPError as e:
                # Log HTTP errors but continue with next ZIP
                self.logger.warning(f"HTTP {e.response.status_code} for ZIP {zip_code}")
                continue
            except requests.RequestException as e:
                # Log request errors but continue with next ZIP
                self.logger.warning(f"Request failed for ZIP {zip_code}: {e}")
                continue
        
        if not all_stores:
            raise ValueError("No stores found")
        
        df = pd.DataFrame(all_stores)
        
        # Drop any records with null coordinates
        df = df.dropna(subset=["latitude", "longitude"])
        
        return df
