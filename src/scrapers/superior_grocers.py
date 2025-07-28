import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
from urllib.parse import unquote
from src.core.base_scraper import BaseScraper


class SuperiorGrocersScraper(BaseScraper):
    """Scraper for Superior Grocers locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape Superior Grocers locations by visiting individual location pages
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        # Headers from notebook
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        
        # Override with config headers if provided
        headers.update(self.config.get('headers', {}))
        
        # Main locations page URL from notebook
        main_url = "https://superiorgrocers.com/locations/"
        base_url = "https://superiorgrocers.com"
        
        print(f"Scraping Superior Grocers from individual location pages...")
        
        try:
            # Get main locations page
            response = self.session.get(
                main_url,
                headers=headers,
                timeout=self.config.get('timeout', 30)
            )
            response.raise_for_status()
            
            # Parse HTML to get individual location URLs
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find all location URLs (from notebook pattern)
            urls = soup.find_all("a", class_="choose")
            location_urls = [
                base_url + loc_url.get("href")
                for loc_url in urls
                if loc_url.get("href") and "location" in loc_url.get("href")
            ]
            
            if not location_urls:
                raise ValueError("No location URLs found on main page")
            
            print(f"Found {len(location_urls)} location pages to scrape...")
            
            locations = []
            
            # Loop through each location page (from notebook pattern)
            for url in tqdm(location_urls, desc="Processing locations"):
                try:
                    location_response = self.session.get(
                        url,
                        headers=headers,
                        timeout=self.config.get('timeout', 30)
                    )
                    location_response.raise_for_status()
                    
                    location_soup = BeautifulSoup(location_response.content, "html.parser")
                    
                    # Extract Google Maps address links (from notebook pattern)
                    address_links = location_soup.find_all(
                        "a", href=re.compile(r"google\.com/maps|maps\.google.com")
                    )
                    
                    google_maps_url = None
                    for link in address_links:
                        if "maps/place" in link["href"] or "?daddr=" in link["href"]:
                            google_maps_url = link["href"]
                            break
                    
                    # Extract JavaScript variables (from notebook pattern)
                    script_content = location_soup.find("script", string=re.compile(r"var latlon = \["))
                    store_name = None
                    latlon = None
                    
                    if script_content:
                        # Extract store name from 'var store = '...'
                        store_match = re.search(r"var store = '(.+?)';", script_content.string)
                        if store_match:
                            store_name = store_match.group(1)
                        
                        # Extract coordinates from 'var latlon = [lat, lng]'
                        latlon_match = re.search(
                            r"var latlon = \[([-\d.]+),\s*([-\d.]+)\];", script_content.string
                        )
                        if latlon_match:
                            latlon = [float(latlon_match.group(1)), float(latlon_match.group(2))]
                    
                    # Parse address from Google Maps URL (from notebook pattern)
                    street, city, state, zip_code = "", "", "", ""
                    if google_maps_url:
                        try:
                            # Clean URL and decode
                            address_clean = google_maps_url.replace("https://www.google.com/maps/place/", "")
                            address_clean = unquote(address_clean)
                            
                            # Split by comma - typical format: "Street, City, State ZIP"
                            if "%2C" in address_clean:
                                parts = address_clean.split("%2C")
                            else:
                                parts = address_clean.split(",")
                            
                            if len(parts) >= 3:
                                street = parts[0].strip()
                                city = parts[1].strip()
                                
                                # Parse state and zip from last part
                                state_zip = parts[2].strip()
                                if "+" in state_zip:
                                    state_zip_parts = state_zip.split("+")
                                    state = state_zip_parts[0].strip()
                                    if len(state_zip_parts) > 1:
                                        zip_code = state_zip_parts[1].strip()
                                else:
                                    # Try to extract state and zip with regex
                                    state_zip_match = re.match(r'^([A-Z]{2})\s+(\d{5}).*$', state_zip)
                                    if state_zip_match:
                                        state = state_zip_match.group(1)
                                        zip_code = state_zip_match.group(2)
                                    else:
                                        state = state_zip
                        except Exception as e:
                            print(f"Error parsing address from URL: {e}")
                    
                    # Build location data (from notebook pattern)
                    location_dict = {
                        'name': store_name or 'Superior Grocers',
                        'address': street,
                        'city': city,
                        'state': state,
                        'zip_code': zip_code,
                        'latitude': latlon[0] if latlon else None,
                        'longitude': latlon[1] if latlon else None,
                        'url': url,
                    }
                    
                    # Only add if we have essential data
                    if store_name or street:
                        locations.append(location_dict)
                    
                except Exception as e:
                    print(f"Error processing location page {url}: {e}")
                    continue
            
            if not locations:
                raise ValueError("No valid locations extracted from individual pages")
            
            # Convert to DataFrame
            df = pd.DataFrame(locations)
            
            # Clean up data
            if 'state' in df.columns:
                df['state'] = df['state'].str.upper().str.strip()
            
            print(f"Found {len(df)} Superior Grocers locations")
            
            return df
            
        except Exception as e:
            print(f"Error scraping Superior Grocers locations: {str(e)}")
            if 'response' in locals():
                print(f"Response status: {response.status_code}")
                print(f"Response URL: {response.url}")
            raise 