import pandas as pd
import requests
import re
from src.core.base_scraper import BaseScraper


class WahoosScraper(BaseScraper):
    """Scraper for Wahoo's locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape Wahoo's locations using their WordPress AJAX API
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        print(f"Fetching Wahoo's locations via WordPress AJAX...")
        
        try:
            # First, try to get the nonce from the locations page
            nonce = self._get_nonce()
            
            # Cookies from notebook (may need to be dynamic)
            cookies = {
                "sbjs_migrations": "1418474375998%3D1",
                "sbjs_first_add": "fd%3D2024-02-17%2017%3A43%3A45%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.wahoos.com%2Fselect-your-region%2F%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.google.com%2F",
                "sbjs_current": "typ%3Dorganic%7C%7C%7Csrc%3Dgoogle%7C%7C%7Cmdm%3Dorganic%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29",
                "sbjs_first": "typ%3Dorganic%7C%7C%7Csrc%3Dgoogle%7C%7C%7Cmdm%3Dorganic%7C%7C%7Ccmp%3D%28none%29%7C%7C%7Ccnt%3D%28none%29%7C%7C%7Ctrm%3D%28none%29%7C%7C%7Cid%3D%28none%29",
                "sbjs_udata": "vst%3D1%7C%7C%7Cuip%3D%28none%29%7C%7C%7Cuag%3DMozilla%2F5.0%20%28Macintosh%3B%20Intel%20Mac%20OS%20X%2010_15_7%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F121.0.0.0%20Safari%2F537.36",
                "_ga": "GA1.1.1252747858.1708191826",
                "sbjs_current_add": "fd%3D2024-02-17%2017%3A43%3A56%7C%7C%7Cep%3Dhttps%3A%2F%2Fwww.wahoos.com%2Fselect-your-region%2F%7C%7C%7Crf%3Dhttps%3A%2F%2Fwww.google.com%2F",
                "sbjs_session": "pgs%3D4%7C%7C%7Ccpg%3Dhttps%3A%2F%2Fwww.wahoos.com%2Flocations%2F%3Fsl-addr%3D90066",
                "_ga_NMTMD45WZB": "GS1.1.1708191826.1.1.1708191864.22.0.0",
            }
            
            # Headers from notebook
            headers = {
                "authority": "www.wahoos.com",
                "accept": "application/json, text/javascript, */*; q=0.01",
                "accept-language": "en-US,en;q=0.9,es;q=0.8",
                "referer": "https://www.wahoos.com/locations/?sl-addr=90066",
                "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"macOS"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "x-requested-with": "XMLHttpRequest",
            }
            
            # Override with config headers if provided
            headers.update(self.config.get('headers', {}))
            
            # Parameters from notebook
            params = {
                "action": "asl_load_stores",
                "nonce": nonce,
                "load_all": "1",
                "layout": "1",
            }
            
            # Make the AJAX request
            response = self.session.get(
                "https://www.wahoos.com/wp-admin/admin-ajax.php",
                params=params,
                cookies=cookies,
                headers=headers,
                timeout=self.config.get('timeout', 30)
            )
            response.raise_for_status()
            
            # Parse JSON response
            data = response.json()
            
            if not data:
                raise ValueError("No locations returned from API")
            
            # Convert to DataFrame (from notebook pattern)
            df = pd.DataFrame(data)
            
            # Select and rename columns to our standard (from notebook)
            if not df.empty:
                column_mapping = {
                    'id': 'store_id',
                    'title': 'name',
                    'street': 'address',
                    'city': 'city',
                    'state': 'state',
                    'postal_code': 'zip_code',
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
            if 'state' in df.columns:
                df['state'] = df['state'].str.upper()
                
            print(f"Found {len(df)} Wahoo's locations")
            
            return df
            
        except Exception as e:
            print(f"Error fetching Wahoo's locations: {str(e)}")
            if 'response' in locals():
                print(f"Response status: {response.status_code}")
                try:
                    print(f"Response content: {response.text[:500]}")
                except:
                    print("Could not display response content")
            raise
    
    def _get_nonce(self) -> str:
        """Extract nonce from the locations page"""
        try:
            # Try the static nonce from notebook first
            static_nonce = "2af9d467f5"
            
            # Get the locations page to extract current nonce
            headers = {
                "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            }
            
            response = self.session.get(
                "https://www.wahoos.com/locations/",
                headers=headers,
                timeout=self.config.get('timeout', 30)
            )
            
            # Look for nonce in JavaScript or hidden fields
            nonce_patterns = [
                r'"nonce":"([^"]+)"',
                r'nonce["\']?\s*[:=]\s*["\']([^"\']+)["\']',
                r'wp_nonce["\']?\s*[:=]\s*["\']([^"\']+)["\']',
            ]
            
            for pattern in nonce_patterns:
                matches = re.findall(pattern, response.text)
                if matches:
                    print(f"Found dynamic nonce: {matches[0]}")
                    return matches[0]
            
            # Fallback to static nonce if dynamic extraction fails
            print(f"Using static nonce: {static_nonce}")
            return static_nonce
            
        except Exception as e:
            print(f"Error extracting nonce, using static: {e}")
            return "2af9d467f5" 