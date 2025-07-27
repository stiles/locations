import pandas as pd
import us
from src.core.base_scraper import BaseScraper


class InNOutScraper(BaseScraper):
    """Scraper for In-N-Out Burger locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape In-N-Out locations from their API
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        # API endpoint from notebook
        url = "https://locations.in-n-out.com/api/finder/search/?latitude=33.985995&longitude=-118.4336666"
        
        # Fetch data
        response = self.session.get(url, timeout=self.config.get('timeout', 30))
        response.raise_for_status()
        
        # Read JSON data
        data = response.json()
        src = pd.DataFrame(data)
        
        if len(src) == 0:
            raise ValueError("No locations returned from API")
        
        # Clean up column names (from notebook)
        src.columns = src.columns.str.lower()
        
        # Process dates (from notebook)
        src["opened_date"] = pd.to_datetime(src["opendate"], errors="coerce").dt.date.astype(str)
        src["opened_year"] = (
            pd.to_datetime(src["opened_date"])
            .dt.year.astype(str)
            .str.replace(".0", "", regex=False)
        )
        
        # Create state name mapping (from notebook)
        state_mapping = {state.abbr: state.name for state in us.states.STATES}
        
        # Select and rename columns to match our standard format
        df = src[
            [
                "storenumber",
                "name", 
                "streetaddress",
                "city",
                "state",
                "zipcode",
                "latitude",
                "longitude",
                "opened_date",
                "opened_year",
            ]
        ].copy()
        
        # Add state names
        df["state_name"] = df["state"].map(state_mapping)
        
        # Rename columns to match our standard format
        df = df.rename(columns={
            "streetaddress": "address",
            "zipcode": "zip_code",
            "storenumber": "store_number"
        })
        
        # Ensure required columns exist and have proper types
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        
        return df 