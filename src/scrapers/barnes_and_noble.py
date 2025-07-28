import pandas as pd
import time
import json
from tqdm import tqdm
from src.core.base_scraper import BaseScraper


class BarnesAndNobleScraper(BaseScraper):
    """Scraper for Barnes & Noble locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape Barnes & Noble locations by iterating through ZIP codes
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        # Load ZIP codes reference file (try both possible locations)
        try:
            zips_df = pd.read_json("_reference/data/zips_reference_pop_gen.json")
        except FileNotFoundError:
            # Try alternative path structure
            zips_df = pd.read_json("_reference/data/zips_reference.json")
        
        zips_df = zips_df.sort_values("population", ascending=False)
        
        # Use the same sampling strategy as the notebook:
        # Top 750 most populous + random sample of 750 from rest = 1500 total
        zips_top = zips_df.head(750)
        zips_sample = zips_df.tail(len(zips_df) - 750).sample(750, random_state=42)
        zips_combined = pd.concat([zips_sample, zips_top]).reset_index(drop=True)
        zips = zips_combined['zip'].to_list()
        
        # Headers from notebook
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9,es;q=0.8',
            'Connection': 'keep-alive',
            'Referer': 'https://stores.barnesandnoble.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'
        }
        
        # Override with config headers if provided
        headers.update(self.config.get('headers', {}))
        
        # API endpoint from notebook
        base_url = "https://stores.barnesandnoble.com/_next/data/q_Dn38wVXgIqr66gMVyVY/index.json"
        
        # Collect stores
        store_list = []
        rate_limit = self.config.get('rate_limit', 1.0)  # Be conservative with rate limiting
        
        print(f"Fetching Barnes & Noble locations from {len(zips)} ZIP codes...")
        
        for i, zip_code in enumerate(tqdm(zips, desc="Processing ZIP codes")):
            params = {
                'searchText': zip_code,
            }
            
            try:
                response = self.session.get(
                    base_url,
                    params=params,
                    headers=headers,
                    timeout=self.config.get('timeout', 30)
                )
                response.raise_for_status()
                
                # Debug the response structure
                data = response.json()
                
                # Try different possible response structures
                stores_json = None
                if 'pageProps' in data and 'stores' in data['pageProps']:
                    stores_data = data['pageProps']['stores']
                    # Try the original structure from notebook
                    if 'content' in stores_data:
                        stores_json = stores_data['content']
                    # Try alternative structures
                    elif isinstance(stores_data, list):
                        stores_json = stores_data
                    elif 'data' in stores_data:
                        stores_json = stores_data['data']
                    elif 'results' in stores_data:
                        stores_json = stores_data['results']
                
                # If we found stores, process them
                if stores_json:
                    for store in stores_json:
                        store_data = {
                            'store_id': store.get('storeId'),
                            'name': store.get('name'),
                            'address': store.get('address1'),
                            'address2': store.get('address2'),
                            'city': store.get('city'),
                            'state': store.get('state'),
                            'zip_code': store.get('zip'),
                            'phone': store.get('phone'),
                        }
                        
                        # Handle location coordinates (could be array or object)
                        location = store.get('location', [])
                        if isinstance(location, list) and len(location) >= 2:
                            store_data['longitude'] = location[0]
                            store_data['latitude'] = location[1]
                        elif isinstance(location, dict):
                            store_data['longitude'] = location.get('lng') or location.get('longitude')
                            store_data['latitude'] = location.get('lat') or location.get('latitude')
                        
                        store_list.append(store_data)
                
            except Exception as e:
                # Log the error but continue with other ZIP codes
                if i < 5:  # Log first few errors for debugging
                    print(f"Error for ZIP {zip_code}: {str(e)}")
                    if 'response' in locals():
                        print(f"Response status: {response.status_code}")
                        try:
                            print(f"Response structure: {list(response.json().keys())}")
                        except:
                            print("Could not parse response JSON")
                continue
            
            # Rate limiting
            if i < len(zips) - 1:
                time.sleep(rate_limit)
        
        if not store_list:
            raise ValueError("No stores found from any ZIP code")
        
        # Convert to DataFrame and deduplicate
        df = pd.DataFrame(store_list).drop_duplicates(subset=['store_id']).reset_index(drop=True)
        
        # Ensure required columns exist
        required_columns = ['name', 'address', 'city', 'state', 'zip_code']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None
        
        # Clean up data types
        if 'latitude' in df.columns:
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        if 'longitude' in df.columns:
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        print(f"Found {len(df)} unique Barnes & Noble locations")
        
        return df 