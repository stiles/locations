"""
Menchies Frozen Yogurt locations scraper

Scrapes store locations from Menchies all-locations page using HTML parsing.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.core.base_scraper import BaseScraper


class MenchiesScraper(BaseScraper):
    """Scraper for Menchies Frozen Yogurt locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Menchies locations from their all-locations page
        
        Returns:
            pd.DataFrame: Location data with columns:
                - name: Store name/location
                - address: Full address  
                - phone: Phone number
                - latitude: Latitude coordinate
                - longitude: Longitude coordinate
        """
        url = "https://www.menchies.com/find-a-store/"
        
        response = self.session.get(url, timeout=self.config.get('timeout', 30))
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        location_list = soup.findAll("div", class_="loc-info")
        
        locations = []
        
        for loc in location_list:
            # Extract directions element for coordinates
            directions = loc.find("p", class_="loc-directions")
            
            if directions:
                # Extract coordinates from Google Maps link
                coords_element = directions.find("a")
                coords = None
                latitude = None
                longitude = None
                
                if coords_element and "href" in coords_element.attrs:
                    href = coords_element["href"]
                    if "https://maps.google.com/?daddr=" in href:
                        coords = href.replace("https://maps.google.com/?daddr=", "")
                        if coords and "," in coords:
                            try:
                                lat_str, lng_str = coords.split(", ", 1)
                                latitude = float(lat_str.strip())
                                longitude = float(lng_str.strip())
                            except (ValueError, IndexError):
                                # Skip if coordinate parsing fails
                                pass

                # Extract location name
                name_element = loc.find("a")
                name = name_element.text.strip() if name_element else None

                # Extract address
                address_element = loc.find("div", class_="loc-address")
                address = None
                if address_element:
                    address = address_element.get_text(separator=", ").strip()

                # Extract phone
                phone_element = loc.find("p", class_="loc-phone")
                phone = phone_element.get_text().strip() if phone_element else None

                # Only add location if we have basic required data
                if name and address:
                    location_data = {
                        "name": name,
                        "address": address,
                        "phone": phone,
                        "latitude": latitude,
                        "longitude": longitude,
                    }
                    locations.append(location_data)

        if not locations:
            raise ValueError("No locations found")

        df = pd.DataFrame(locations)
        
        # Parse address into city/state components
        self._parse_address_components(df)
        
        return df

    def _parse_address_components(self, df: pd.DataFrame) -> None:
        """Parse address field into city and state components"""
        # Split address on commas and extract city/state from the end
        address_parts = df["address"].str.split(", ")
        
        df["city"] = None
        df["state"] = None
        
        for idx, parts in enumerate(address_parts):
            if parts and len(parts) >= 2:
                # For US locations, state is typically the second to last part
                # For international, we may not have state
                if len(parts) >= 3:
                    df.loc[idx, "city"] = parts[-2].strip()
                    df.loc[idx, "state"] = parts[-1].strip()[:2].upper()  # Take first 2 chars for state abbrev
                elif len(parts) == 2:
                    df.loc[idx, "city"] = parts[-1].strip()
                    df.loc[idx, "state"] = ""  # International locations may not have state
