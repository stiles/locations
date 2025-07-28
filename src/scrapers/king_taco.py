import pandas as pd
from src.core.base_scraper import BaseScraper


class KingTacoScraper(BaseScraper):
    """Scraper for King Taco locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape King Taco locations using their WordPress AJAX API
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        # Headers from notebook
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        
        # Override with config headers if provided
        headers.update(self.config.get('headers', {}))
        
        # Parameters from notebook - single request with large radius gets all locations
        params = {
            "action": "store_search",
            "lat": "33.98896",        # Los Angeles area (King Taco's center)
            "lng": "-118.4165",
            "max_results": "100",     # Should be enough for all 21 locations
            "search_radius": "1000",  # 1000 mile radius covers entire US
            "skip_cache": "1",
        }
        
        # API endpoint from notebook  
        url = "https://kingtaco.com/wp-admin/admin-ajax.php"
        
        print(f"Fetching all King Taco locations with single API call...")
        
        try:
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=self.config.get('timeout', 30)
            )
            response.raise_for_status()
            
            # Get JSON response
            data = response.json()
            
            if not data:
                raise ValueError("No locations returned from API")
                
            # Convert directly to DataFrame (from notebook pattern)
            df = pd.DataFrame(data)
            
            # Clean up data as in notebook
            if 'state' in df.columns:
                df['state'] = df['state'].str.upper()
            
            # Select and rename columns to match our standard (from notebook)
            if not df.empty:
                column_mapping = {
                    'id': 'store_id',
                    'store': 'name', 
                    'address': 'address',
                    'city': 'city',
                    'state': 'state',
                    'zip': 'zip_code',
                    'lat': 'latitude',
                    'lng': 'longitude',
                    'phone': 'phone',
                }
                
                # Only rename columns that exist
                available_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
                df = df.rename(columns=available_mapping)
                
                # Select standard columns
                standard_columns = ['store_id', 'name', 'address', 'city', 'state', 'zip_code', 'latitude', 'longitude', 'phone']
                existing_columns = [col for col in standard_columns if col in df.columns]
                df = df[existing_columns].copy()
            
            # Clean up data types
            if 'latitude' in df.columns:
                df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
            if 'longitude' in df.columns:
                df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
                
            print(f"Found {len(df)} King Taco locations")
            
            return df
            
        except Exception as e:
            print(f"Error fetching King Taco locations: {str(e)}")
            if 'response' in locals():
                print(f"Response status: {response.status_code}")
                try:
                    print(f"Response content: {response.text[:500]}")
                except:
                    print("Could not display response content")
            raise 