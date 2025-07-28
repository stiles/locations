import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from urllib.parse import unquote
from src.core.base_scraper import BaseScraper


class BucEesScraper(BaseScraper):
    """Scraper for Buc-ee's locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape Buc-ee's locations by parsing their locations HTML page
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        # Headers from notebook
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        
        # Override with config headers if provided
        headers.update(self.config.get('headers', {}))
        
        # URL from notebook
        url = "https://buc-ees.com/locations/"
        
        print(f"Scraping Buc-ee's locations from HTML page...")
        
        try:
            response = self.session.get(
                url,
                headers=headers,
                timeout=self.config.get('timeout', 30)
            )
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Extract store details from h4 tags (from notebook pattern)
            store_details = []
            for details in soup.find_all("h4"):
                store_text = details.text.strip()
                if store_text:  # Only add non-empty details
                    store_details.append(store_text)
            
            # Extract directions URLs (from notebook pattern)
            directions_list = []
            for address_div in soup.find_all("div", class_="bucees-location-directions"):
                link = address_div.find("a")
                if link and link.get("href"):
                    directions_list.append(link["href"])
            
            if len(store_details) != len(directions_list):
                print(f"Warning: Mismatch between store details ({len(store_details)}) and directions ({len(directions_list)})")
                # Take the minimum to avoid index errors
                min_length = min(len(store_details), len(directions_list))
                store_details = store_details[:min_length]
                directions_list = directions_list[:min_length]
            
            if not store_details:
                raise ValueError("No store details found in HTML")
            
            # Create DataFrame from extracted data (from notebook pattern)
            df = pd.DataFrame({
                'store_details': store_details,
                'url': directions_list
            })
            
            # Split store details (from notebook: "store_number – city, state")
            if 'store_details' in df.columns:
                split_details = df['store_details'].str.split(' – ', expand=True)
                if split_details.shape[1] >= 2:
                    df['store_number'] = split_details[0]
                    df['city_state'] = split_details[1]
                else:
                    # Fallback if format is different
                    df['store_number'] = df['store_details'] 
                    df['city_state'] = ''
            
            # Parse Google Maps URLs to get address (from notebook pattern)
            if 'url' in df.columns:
                # Extract address from Google Maps URL path (5th segment in URL)
                df['address_full'] = df['url'].str.split('/', expand=True)[5]
                # URL decode the address
                df['address_full'] = df['address_full'].apply(lambda x: unquote(str(x)) if pd.notna(x) else '')
            
            # Parse city_state into separate columns
            if 'city_state' in df.columns:
                city_state_split = df['city_state'].str.split(', ', expand=True)
                if city_state_split.shape[1] >= 2:
                    df['city'] = city_state_split[0]
                    df['state'] = city_state_split[1]
                else:
                    df['city'] = df['city_state']
                    df['state'] = ''
            
            # Clean up and standardize columns
            standardized_df = pd.DataFrame({
                'store_id': df.get('store_number', ''),
                'name': df.get('store_number', '').apply(lambda x: f"Buc-ee's {x}" if x else "Buc-ee's"),
                'address': df.get('address_full', ''),
                'city': df.get('city', ''),
                'state': df.get('state', ''),
                'directions_url': df.get('url', ''),
            })
            
            # Clean state column
            if 'state' in standardized_df.columns:
                standardized_df['state'] = standardized_df['state'].str.upper().str.strip()
            
            print(f"Found {len(standardized_df)} Buc-ee's locations")
            
            return standardized_df
            
        except Exception as e:
            print(f"Error scraping Buc-ee's locations: {str(e)}")
            if 'response' in locals():
                print(f"Response status: {response.status_code}")
                print(f"Response URL: {response.url}")
            raise 