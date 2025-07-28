import pandas as pd
import time
import json
from tqdm import tqdm
from src.core.base_scraper import BaseScraper


class CVSScraper(BaseScraper):
    """Scraper for CVS Pharmacy locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape CVS locations by iterating through ZIP codes
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        # Load ZIP codes reference file (from notebook pattern)
        try:
            zips_df = pd.read_json("_reference/data/zip_code_demographics_esri.json")
        except FileNotFoundError:
            # Fallback to other available files
            zips_df = pd.read_json("_reference/data/zips_reference_pop_gen.json")
            zips_df = zips_df.rename(columns={'population': 'population'})
        
        # Filter and sort by population (notebook uses >5000 population)
        zips_df = (
            zips_df.query("population > 5000")
            .sort_values("population", ascending=False)
            .reset_index(drop=True)
        )
        
        # Ensure 5-digit ZIP codes
        if 'zipcode' in zips_df.columns:
            zips_df['zipcode'] = zips_df['zipcode'].astype(str).str.zfill(5)
            zip_col = 'zipcode'
        else:
            zips_df['zip'] = zips_df['zip'].astype(str).str.zfill(5)
            zip_col = 'zip'
        
        # Use the same sampling strategy as notebook: top 4000 + bottom 4000 = 8000 total
        top_zips = zips_df[zip_col].head(4000).to_list()
        bottom_zips = zips_df[zip_col].tail(4000).to_list()
        zips_list = top_zips + bottom_zips
        
        # Headers from notebook (including API key)
        headers = {
            "authority": "www.cvs.com",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "x-api-key": "k6DnPo1puMOQmAhSCiRGYvzMYOSFu903",
        }
        
        # Override with config headers if provided
        headers.update(self.config.get('headers', {}))
        
        # API endpoint from notebook
        base_url = "https://www.cvs.com/api/locator/v2/stores/search"
        
        # Collect stores
        all_stores = []
        rate_limit = self.config.get('rate_limit', 1.0)
        
        print(f"Fetching CVS locations from {len(zips_list)} ZIP codes...")
        
        for i, zip_code in enumerate(tqdm(zips_list, desc="Processing ZIP codes")):
            params = {
                "searchBy": "USER-TEXT",
                "searchText": f"{zip_code}",
                "searchRadiusInMiles": "1000",
                "maxItemsInResult": "1000", 
                "resultsPerPage": "1000",
            }
            
            try:
                response = self.session.get(
                    base_url,
                    params=params,
                    headers=headers,
                    timeout=self.config.get('timeout', 30)
                )
                response.raise_for_status()
                
                # Extract stores from response
                data = response.json()
                store_list = data.get("storeList", [])
                
                if store_list:
                    for store in store_list:
                        # Extract store data with new API structure
                        address_info = store.get('address', {})
                        store_info = store.get('storeInfo', {})
                        hours_info = store.get('hours', {})
                        indicators = store.get('indicators', [])
                        
                        # Get phone number from nested structure
                        phone_numbers = store_info.get('phoneNumbers', [{}])
                        phone = None
                        if phone_numbers and isinstance(phone_numbers, list):
                            # Try to get retail phone first, then any available
                            phone = phone_numbers[0].get('retail') or phone_numbers[0].get('pharmacy')
                        
                        store_data = {
                            'store_id': store_info.get('storeId'),
                            'name': f"CVS Pharmacy #{store_info.get('storeId', '')}",  # CVS doesn't provide store names in API
                            'address': address_info.get('street'),
                            'address2': None,  # Not provided in new API
                            'city': address_info.get('city'),
                            'state': address_info.get('state'),
                            'zip_code': address_info.get('zip'),
                            'phone': phone,
                            'latitude': store_info.get('latitude'),
                            'longitude': store_info.get('longitude'),
                        }
                        
                        # Additional CVS-specific fields from new API
                        store_data.update({
                            'store_type': store_info.get('storeType'),
                            'distance': store_info.get('distance'),
                            'fax_number': store_info.get('faxNumber'),
                            'time_zone': hours_info.get('timeZone'),
                            'services': indicators,  # List of service indicators
                            'intersection': address_info.get('intersection'),
                        })
                        
                        all_stores.append(store_data)
                
            except Exception as e:
                # Log the error but continue with other ZIP codes
                if i < 5:  # Log first few errors for debugging
                    print(f"Error for ZIP {zip_code}: {str(e)}")
                    if 'response' in locals():
                        print(f"Response status: {response.status_code}")
                        try:
                            error_data = response.json()
                            print(f"Error response: {error_data}")
                        except:
                            print("Could not parse error response")
                continue
            
            # Rate limiting
            if i < len(zips_list) - 1:
                time.sleep(rate_limit)
        
        if not all_stores:
            raise ValueError("No stores found from any ZIP code")
        
        # Convert to DataFrame and deduplicate
        df = pd.DataFrame(all_stores).drop_duplicates(subset=['store_id']).reset_index(drop=True)
        
        # Clean up data types
        if 'latitude' in df.columns:
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        if 'longitude' in df.columns:
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        print(f"Found {len(df)} unique CVS locations")
        
        return df 