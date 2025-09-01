"""
Cinemark theaters scraper

Scrapes theater locations from Cinemark's full theater list and individual pages.
"""

import pandas as pd
import requests
import json
import time
import logging
from bs4 import BeautifulSoup
from src.core.base_scraper import BaseScraper


class CinemarkScraper(BaseScraper):
    """Scraper for Cinemark theater locations"""

    def __init__(self, company_name: str, config: dict):
        super().__init__(company_name, config)
        self.logger = logging.getLogger(__name__)

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Cinemark theater locations using directory listing approach
        
        Returns:
            pd.DataFrame: Theater location data with standardized columns
        """
        base_url = "https://www.cinemark.com"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        # Update session headers
        self.session.headers.update(headers)
        
        # Step 1: Get all theater links from the full theater list
        self.logger.info("Fetching theater list from full-theatre-list page")
        try:
            response = self.session.get(
                "https://www.cinemark.com/full-theatre-list", 
                timeout=self.config.get('timeout', 30)
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            theater_list = soup.find_all("div", class_="theatres-by-state")
            
            links = []
            for theater_group in theater_list:
                for link_tag in theater_group.find_all("a"):
                    href = link_tag.get("href")
                    if href:
                        full_url = base_url + href
                        # Filter out closed locations
                        if "closed" not in full_url.lower():
                            links.append(full_url)
            
            self.logger.info(f"Found {len(links)} theater links")
            
        except Exception as e:
            self.logger.error(f"Error fetching theater list: {e}")
            raise
        
        # Step 2: Visit each theater page and extract details
        theater_details = []
        
        for i, link in enumerate(links):
            try:
                self.logger.info(f"Processing theater {i+1}/{len(links)}: {link}")
                
                response = self.session.get(link, timeout=self.config.get('timeout', 30))
                response.raise_for_status()
                
                link_soup = BeautifulSoup(response.text, "html.parser")
                
                # Look for JSON-LD structured data
                script_tag = link_soup.find("script", type="application/ld+json")
                if script_tag and script_tag.string:
                    try:
                        json_data = json.loads(script_tag.string.strip())
                        
                        # Extract coordinates from image tag if available
                        latitude, longitude = None, None
                        img_tag = link_soup.find("img", class_="img-responsive lazyload")
                        if img_tag and "data-src" in img_tag.attrs:
                            src_data = img_tag["data-src"]
                            if "=" in src_data and "," in src_data:
                                try:
                                    coord_part = src_data.split("=")[2]
                                    lat_str, lon_str = coord_part.split(",")[0], coord_part.split(",")[1].replace("&key", "")
                                    latitude = float(lat_str)
                                    longitude = float(lon_str)
                                except (IndexError, ValueError):
                                    pass
                        
                        # Extract address information from JSON-LD
                        address_data = json_data.get("address", [{}])[0] if isinstance(json_data.get("address"), list) else json_data.get("address", {})
                        
                        theater_dict = {
                            "name": json_data.get("name"),
                            "address": address_data.get("streetAddress"),
                            "city": address_data.get("addressLocality"),
                            "state": address_data.get("addressRegion"),
                            "zip_code": address_data.get("postalCode"),
                            "phone": json_data.get("telephone"),
                            "latitude": latitude,
                            "longitude": longitude,
                            "url": link,
                        }
                        
                        theater_details.append(theater_dict)
                        
                    except (json.JSONDecodeError, KeyError) as e:
                        self.logger.warning(f"Error parsing JSON-LD for {link}: {e}")
                        continue
                else:
                    self.logger.warning(f"No JSON-LD script found for {link}")
                    continue
                
                # Rate limiting - be respectful
                time.sleep(0.5)
                
            except requests.HTTPError as e:
                # Log HTTP errors but continue with next theater
                self.logger.warning(f"HTTP Error for {link}: {e}")
                continue
            except Exception as e:
                # Log other errors but continue with next theater
                self.logger.warning(f"Error processing {link}: {e}")
                continue
        
        if not theater_details:
            raise ValueError("No theater locations found")
        
        df = pd.DataFrame(theater_details)
        
        # Clean up the data
        df = df.dropna(subset=['name', 'address', 'city', 'state'])  # Remove rows missing essential data
        
        self.logger.info(f"Successfully scraped {len(df)} theater locations")
        return df
