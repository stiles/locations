"""
Shipley Do-nuts locations scraper

Scrapes store locations from Shipley Do-nuts locations page using inline JavaScript JSON extraction.
"""

import pandas as pd
import requests
import json
import re
from bs4 import BeautifulSoup
from src.core.base_scraper import BaseScraper


class ShipleyDonutsScraper(BaseScraper):
    """Scraper for Shipley Do-nuts locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Shipley Do-nuts locations from their locations page
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        url = "https://shipleydonuts.com/locations/"
        
        response = self.session.get(url, timeout=self.config.get('timeout', 30))
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Find script tag containing location data
        script_tag = soup.find("script", string=lambda t: t and "var locations_meta =" in t)
        
        if not script_tag:
            raise ValueError("Could not find script tag with location data")
        
        # Extract JSON from script tag
        script_content = script_tag.string.strip()
        
        # Clean up the JavaScript to extract just the JSON
        json_content = script_content.replace("var locations_meta = ", "")
        
        # Remove any trailing JavaScript code
        json_content = re.sub(r'var is_single_location_post = false;.*$', '', json_content)
        
        # Fix any malformed JSON (based on notebook pattern)
        json_content = json_content.replace('\/"}];', '\/"}]')
        
        try:
            locations_data = json.loads(json_content)
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON data: {e}")
        
        if not locations_data:
            raise ValueError("No location data found")
        
        df = pd.DataFrame(locations_data)
        
        # Flatten nested location data if it exists
        if 'location' in df.columns:
            location_df = pd.json_normalize(df['location'])
            df = pd.concat([df.drop(columns=['location']), location_df], axis=1)
        
        # Standardize column names
        column_mapping = {
            'lat': 'latitude',
            'lng': 'longitude',
            'name': 'name',
            'address': 'address',
            'city': 'city',
            'state': 'state',
            'postal_code': 'zip_code',
            'zip_code': 'zip_code',
            'phone': 'phone'
        }
        
        # Apply column mapping where columns exist
        df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
        
        return df
