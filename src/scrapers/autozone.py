"""
AutoZone locations scraper

Scrapes store locations from AutoZone's Yext API using ZIP code iteration with pagination.
"""

import pandas as pd
import requests
import time
from src.core.base_scraper import BaseScraper


class AutoZoneScraper(BaseScraper):
    """Scraper for AutoZone locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape AutoZone locations using ZIP code iteration with Yext API
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        # Load ZIP codes for iteration - use population > 5000 as in original notebook
        zips_df = pd.read_json("_reference/data/zips_reference_pop_gen.json")
        
        # Filter to larger ZIP codes and get top 20 per state for good coverage
        zips_filtered = zips_df[zips_df["population"] > 5000].sort_values("population", ascending=False)
        top_zipcodes_by_state = (
            zips_filtered.groupby("state")
            .apply(lambda x: x.nlargest(20, "population"), include_groups=False)
            .reset_index(drop=True)
        )
        
        zips = top_zipcodes_by_state["zip"].astype(str).str.zfill(5).to_list()
        
        headers = {
            "authority": "liveapi.yext.com",
            "accept": "application/json, text/plain, */*",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        
        # Update session headers
        self.session.headers.update(headers)
        
        all_stores = []
        seen_store_ids = set()  # Track unique stores to avoid duplicates
        
        for zip_code in zips:
            try:
                offset = 0
                total_results = float("inf")
                
                # Handle pagination for this ZIP code
                while offset < total_results:
                    params = {
                        "location": zip_code,
                        "api_key": "a427dc0cb3e4f080da0ebe74621b8020",  # Public API key from notebook
                        "v": "20180731",
                        "radius": "100",
                        "filters": '[{"countryCode":{"includes":["US","PR","VI"]}}]',
                        "offset": offset,
                    }
                    
                    response = self.session.get(
                        "https://liveapi.yext.com/v2/accounts/me/locations/geosearch",
                        params=params,
                        timeout=self.config.get('timeout', 30)
                    )
                    response.raise_for_status()
                    
                    data = response.json()
                    
                    # Check if response has expected structure
                    if "response" not in data or "locations" not in data["response"]:
                        break
                    
                    locations_data = data["response"]["locations"]
                    total_results = data["response"].get("count", 0)
                    
                    for location in locations_data:
                        store_id = location.get("id")
                        
                        # Skip if we've already seen this store
                        if store_id in seen_store_ids:
                            continue
                            
                        seen_store_ids.add(store_id)
                        
                        store_dict = {
                            "store_id": store_id,
                            "address": location.get("address"),
                            "city": location.get("city"),
                            "state": location.get("state"),
                            "zip_code": location.get("zip"),
                            "phone": location.get("phone"),
                            "timezone": location.get("timezone"),
                            "url": location.get("websiteUrl"),
                            "latitude": location.get("displayLat"),
                            "longitude": location.get("displayLng"),
                        }
                        all_stores.append(store_dict)
                    
                    # Update offset for pagination
                    offset += len(locations_data)
                    
                    # If no more results, break
                    if len(locations_data) == 0:
                        break
                
                # Rate limiting - be respectful to Yext API
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
        
        df = pd.DataFrame(all_stores).drop_duplicates()
        
        # Add name field since AutoZone is the company
        df["name"] = "AutoZone"
        
        return df
