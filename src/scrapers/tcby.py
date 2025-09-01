"""
TCBY Frozen Yogurt locations scraper

Scrapes store locations from TCBY's state-by-state API endpoints.
"""

import pandas as pd
import requests
from src.core.base_scraper import BaseScraper


class TcbyScraper(BaseScraper):
    """Scraper for TCBY Frozen Yogurt locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape TCBY locations by iterating through all US states
        
        Returns:
            pd.DataFrame: Location data with all available columns from API
        """
        # List of all US state abbreviations
        states = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
        ]
        
        all_locations = []
        
        for state in states:
            try:
                locations = self._fetch_state_locations(state)
                if locations:
                    all_locations.extend(locations)
                    
                # Small delay to be respectful
                import time
                time.sleep(0.1)
                
            except Exception as e:
                # Skip states that fail - some may not have locations
                continue

        if not all_locations:
            raise ValueError("No locations found across any states")

        df = pd.DataFrame(all_locations)
        return df

    def _fetch_state_locations(self, state: str) -> list:
        """
        Fetch locations for a specific state
        
        Args:
            state: Two-letter state abbreviation
            
        Returns:
            list: List of location dictionaries
        """
        url = f"https://www.tcby.com/api/geo/{state}/"
        
        response = self.session.get(url, timeout=self.config.get('timeout', 30))
        response.raise_for_status()
        
        if response.status_code == 200:
            return response.json()
        else:
            return []
