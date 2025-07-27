import pandas as pd
import time
from tqdm import tqdm
from src.core.base_scraper import BaseScraper


class StarbucksScraper(BaseScraper):
    """Scraper for Starbucks locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape Starbucks locations by iterating through ZIP codes
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        # Load ZIP codes reference file (from notebook)
        zips_df = pd.read_csv("_reference/data/zips_reference.csv").sort_values(
            "pop2010", ascending=False
        )
        
        # Make a list of ZIP codes (limit to first 1000 for testing)
        # In production, could use full list: zips[0:10000] 
        test_limit = 1000
        zips = list(zips_df.zip[:test_limit])
        
        # Headers from notebook
        headers = {
            "authority": "www.starbucks.com",
            "accept": "application/json", 
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }
        
        # Override with config headers if provided
        headers.update(self.config.get('headers', {}))
        
        # API endpoint
        base_url = "https://www.starbucks.com/bff/locations"
        
        # Loop through ZIP codes and collect stores
        stores_zips_list = []
        rate_limit = self.config.get('rate_limit', 0.5)
        
        print(f"Fetching Starbucks locations from {len(zips)} ZIP codes...")
        
        for i, z in enumerate(tqdm(zips, desc="Processing ZIP codes")):
            params = {"place": f"{z}"}
            
            try:
                response = self.session.get(
                    base_url, 
                    params=params, 
                    headers=headers,
                    timeout=self.config.get('timeout', 45)
                )
                response.raise_for_status()
                
                # Extract stores from response
                data = response.json()
                stores = data.get("stores", [])
                
                if stores:
                    src = pd.DataFrame(stores)
                    stores_zips_list.append(src)
                    
            except Exception as e:
                # Continue on errors (some ZIP codes may not have stores)
                continue
            
            # Rate limiting
            if i < len(zips) - 1:
                time.sleep(rate_limit)
        
        if not stores_zips_list:
            raise ValueError("No stores found from any ZIP code")
            
        # Concatenate all dataframes and deduplicate (from notebook)
        src_df = (
            pd.concat(stores_zips_list)[
                [
                    "storeNumber",
                    "name", 
                    "coordinates",
                    "address",
                    "timeZoneInfo",
                    "ownershipTypeCode",
                    "addressLines",
                    "slug",
                ]
            ]
            .drop_duplicates(subset="storeNumber")
            .reset_index(drop=True)
        )
        
        # Flatten nested columns (from notebook)
        src_df[
            [
                "streetAddressLine1", 
                "streetAddressLine2",
                "streetAddressLine3",
                "city",
                "countrySubdivisionCode",
                "countryCode", 
                "postalCode",
            ]
        ] = pd.json_normalize(src_df["address"])
        
        src_df["timezone"] = pd.json_normalize(src_df["timeZoneInfo"])["olsonTimeZoneId"]
        src_df[["latitude", "longitude"]] = pd.json_normalize(src_df["coordinates"])
        
        # Five-digit ZIP codes
        src_df["zip"] = src_df["postalCode"].str[:5]
        
        # Clean dataframe (from notebook)
        df = (
            src_df.drop(
                [
                    "address",
                    "timeZoneInfo", 
                    "addressLines",
                    "streetAddressLine2",
                    "streetAddressLine3",
                    "coordinates",
                    "postalCode",
                ],
                axis=1,
            )
            .rename(
                columns={
                    "countrySubdivisionCode": "state",
                    "streetAddressLine1": "address", 
                    "storeNumber": "store_number",
                    "countryCode": "country",
                    "ownershipTypeCode": "ownership_type",
                }
            )[
                [
                    "store_number",
                    "name",
                    "ownership_type", 
                    "slug",
                    "address",
                    "city",
                    "state",
                    "zip",
                    "country",
                    "timezone",
                    "latitude",
                    "longitude",
                ]
            ]
            .copy()
        )
        
        # Ensure required columns exist and have proper types
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        
        # Rename zip to zip_code to match our standard
        df = df.rename(columns={"zip": "zip_code"})
        
        print(f"Found {len(df)} unique Starbucks locations")
        
        return df 