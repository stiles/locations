import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from src.core.base_scraper import BaseScraper


class AuBonPainScraper(BaseScraper):
    """Scraper for Au Bon Pain locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape Au Bon Pain locations from their all-stores HTML page
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        # Headers for the request
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        
        # Override with config headers if provided
        headers.update(self.config.get('headers', {}))
        
        # URL from user's discovery
        url = "https://www.aubonpain.com/stores/all-stores"
        
        print(f"Scraping Au Bon Pain locations from HTML page...")
        
        try:
            response = self.session.get(
                url,
                headers=headers,
                timeout=self.config.get('timeout', 30)
            )
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find all location boxes (from HTML structure in user's message)
            location_boxes = soup.find_all("div", class_="loc-box")
            
            if not location_boxes:
                raise ValueError("No location boxes found in HTML")
            
            locations = []
            
            for box in location_boxes:
                try:
                    # Extract store name from h3 with class "loc-box-heading"
                    name_elem = box.find("h3", class_="loc-box-heading")
                    name = name_elem.text.strip() if name_elem else ""
                    
                    # Extract address from dd tags under "Address" dt
                    address_dds = []
                    address_dt = box.find("dt", class_="loc-box-label", string="Address")
                    if address_dt:
                        # Find all dd siblings after the Address dt
                        for sibling in address_dt.find_next_siblings():
                            if sibling.name == "dd":
                                address_dds.append(sibling.text.strip())
                            elif sibling.name == "dt":
                                break  # Stop at next dt element
                    
                    # Join address parts
                    if len(address_dds) >= 2:
                        address = address_dds[0]  # First line is street address
                        city_state_zip = address_dds[1]  # Second line is "City, State ZIP"
                    else:
                        address = ", ".join(address_dds) if address_dds else ""
                        city_state_zip = ""
                    
                    # Parse city, state, zip from city_state_zip
                    city, state, zip_code = "", "", ""
                    if city_state_zip:
                        # Split by comma first to separate city from state+zip
                        parts = city_state_zip.split(", ")
                        if len(parts) >= 2:
                            city = parts[0].strip()
                            state_zip = parts[1].strip()
                            
                            # Extract state and zip from "STATE ZIP" format
                            state_zip_match = re.match(r'^([A-Z]{2})\s+(\d{5}).*$', state_zip)
                            if state_zip_match:
                                state = state_zip_match.group(1)
                                zip_code = state_zip_match.group(2)
                            else:
                                # Fallback - treat as state if it's 2 letters
                                if len(state_zip) == 2 and state_zip.isalpha():
                                    state = state_zip
                    
                    # Extract phone from dd tag under "Phone" dt
                    phone = ""
                    phone_dt = box.find("dt", class_="loc-box-label", string="Phone")
                    if phone_dt:
                        phone_dd = phone_dt.find_next_sibling("dd")
                        if phone_dd:
                            # Extract from <a href="tel:..."> or plain text
                            phone_link = phone_dd.find("a", class_="phonenumber")
                            if phone_link:
                                phone = phone_link.text.strip()
                            else:
                                phone = phone_dd.text.strip()
                    
                    # Create location data
                    location_data = {
                        'name': name,
                        'address': address,
                        'city': city,
                        'state': state,
                        'zip_code': zip_code,
                        'phone': phone,
                    }
                    
                    # Only add if we have essential data
                    if name and (address or city):
                        locations.append(location_data)
                        
                except Exception as e:
                    print(f"Error parsing location box: {e}")
                    continue
            
            if not locations:
                raise ValueError("No valid locations extracted from HTML")
            
            # Convert to DataFrame
            df = pd.DataFrame(locations)
            
            # Clean up data
            if 'state' in df.columns:
                df['state'] = df['state'].str.upper().str.strip()
            
            if 'phone' in df.columns:
                # Clean phone numbers - remove non-digits except for empty values
                df['phone'] = df['phone'].apply(lambda x: re.sub(r'[^\d]', '', str(x)) if x else '')
                df['phone'] = df['phone'].replace('', None)  # Replace empty strings with None
            
            print(f"Found {len(df)} Au Bon Pain locations")
            
            return df
            
        except Exception as e:
            print(f"Error scraping Au Bon Pain locations: {str(e)}")
            if 'response' in locals():
                print(f"Response status: {response.status_code}")
                print(f"Response URL: {response.url}")
            raise 