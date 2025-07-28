import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import json
from src.core.base_scraper import BaseScraper


class HmartScraper(BaseScraper):
    """Scraper for Hmart locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape Hmart locations from embedded JSON data in their main store page
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        # Headers from notebook
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        
        # Override with config headers if provided
        headers.update(self.config.get('headers', {}))
        
        # Main store page URL (updated from redirect)
        url = "https://www.hmart.com/stores"
        
        print(f"Scraping Hmart locations from embedded JSON data...")
        
        try:
            response = self.session.get(
                url,
                headers=headers,
                timeout=self.config.get('timeout', 30)
            )
            response.raise_for_status()
            
            # Parse HTML content
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Find all scripts with xml="space" attribute (from notebook pattern)
            scripts = soup.find_all("script", attrs={"xml": "space"})
            
            if len(scripts) <= 1:
                raise ValueError("Could not find expected script tags with store data")
            
            # Extract JSON data from the second script (index 1) (from notebook)
            script_text = scripts[1].text.strip()
            
            # Use regex to extract the JSON data (from notebook pattern)
            match = re.search(r"jsonLocations:\s*(\{.*\})", script_text)
            if not match:
                raise ValueError("Could not find jsonLocations data in script")
            
            json_data = match.group(1)
            
            # Parse the JSON data (from notebook pattern)
            try:
                # Clean up the JSON (from notebook: replace "}        }" with "}")
                cleaned_json = json_data.replace("}        }", "}")
                store_info = json.loads(cleaned_json)
                
                # Extract store information from the 'items' key (from notebook)
                store_items = store_info.get("items", [])
                
                if not store_items:
                    raise ValueError("No store items found in JSON data")
                
                # Build stores data list (from notebook pattern)
                stores_data = []
                
                for store in store_items:
                    store_dict = {
                        "store_id": store.get("id"),
                        "name": store.get("name"),
                        "address": store.get("address"),
                        "latitude": store.get("lat"),
                        "longitude": store.get("lng"),
                        "phone": store.get("phone"),
                    }
                    stores_data.append(store_dict)
                
                # Convert to DataFrame
                df = pd.DataFrame(stores_data)
                
                # Clean up address formatting (from notebook pattern)
                if 'address' in df.columns:
                    df["address"] = df["address"].str.replace(".", ". ")
                
                # Clean up data types
                if 'latitude' in df.columns:
                    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
                if 'longitude' in df.columns:
                    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
                
                # Clean phone numbers
                if 'phone' in df.columns:
                    df['phone'] = df['phone'].astype(str).str.strip()
                    df['phone'] = df['phone'].replace('', None)
                
                print(f"Found {len(df)} Hmart locations")
                
                return df
                
            except json.JSONDecodeError as e:
                raise ValueError(f"Error decoding JSON data: {e}")
                
        except Exception as e:
            print(f"Error scraping Hmart locations: {str(e)}")
            if 'response' in locals():
                print(f"Response status: {response.status_code}")
                print(f"Response URL: {response.url}")
                if 'script_text' in locals():
                    print(f"Script preview: {script_text[:200]}...")
            raise 