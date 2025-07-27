import pandas as pd
from src.core.base_scraper import BaseScraper


class TraderJoesScraper(BaseScraper):
    """Scraper for Trader Joe's locations"""
    
    def scrape(self) -> pd.DataFrame:
        """
        Scrape Trader Joe's locations using their API
        
        Returns:
            pd.DataFrame: Raw location data with standardized columns
        """
        # Headers from notebook
        headers = {
            "authority": "alphaapi.brandify.com",
            "accept": "application/json, text/plain, */*",
            "referer": "https://www.traderjoes.com/",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
        }
        
        # Override with config headers if provided
        headers.update(self.config.get('headers', {}))
        
        # JSON payload from notebook (with large search radius to get all locations)
        json_data = {
            "request": {
                "appkey": "8BC3433A-60FC-11E3-991D-B2EE0C70A832",
                "formdata": {
                    "geoip": False,
                    "dataview": "store_default", 
                    "limit": 1000,
                    "geolocs": {
                        "geoloc": [
                            {
                                "addressline": "90066",
                                "country": "US",
                                "latitude": "",
                                "longitude": "",
                            },
                        ],
                    },
                    "searchradius": "5000",
                    "where": {
                        "warehouse": {
                            "distinctfrom": "1",
                        },
                    },
                    "false": "0",
                },
            },
        }
        
        # API endpoint
        url = "https://alphaapi.brandify.com/rest/locatorsearch"
        
        # Make request
        response = self.session.post(
            url,
            headers=headers,
            json=json_data,
            timeout=self.config.get('timeout', 30)
        )
        response.raise_for_status()
        
        # Extract data from response
        data = response.json()
        collection = data.get("response", {}).get("collection", [])
        
        if not collection:
            raise ValueError("No locations found in API response")
            
        # Create dataframe
        src = pd.DataFrame(collection)
        
        # Select relevant columns (from notebook)
        df = src[
            [
                "uid",
                "name",
                "address1", 
                "city",
                "state",
                "postalcode",
                "phone",
                "latitude",
                "longitude",
                "beer",
                "liquor", 
                "wineshop",
                "regions",
            ]
        ].copy()
        
        # Rename columns to standard format (from notebook)
        df.rename(columns={"address1": "address", "postalcode": "zip_code"}, inplace=True)
        
        # Extract store number from name (from notebook) 
        df["store_number"] = (
            df["name"]
            .str[-4:]
            .str.strip()
            .str.replace(")", "", regex=False)
            .str.replace("(", "", regex=False)
            .astype(str)
        )
        
        # Clean up name by removing store number in parentheses (from notebook)
        df["name"] = df["name"].str.split("(", expand=True)[0].str.strip()
        
        # Ensure required columns exist and have proper types
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        
        # Make uid absolute value and string (from notebook)
        df["uid"] = df["uid"].abs().astype(str)
        
        print(f"Found {len(df)} Trader Joe's locations")
        
        return df 