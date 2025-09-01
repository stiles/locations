"""
Nike Stores locations scraper

Scrapes store locations from Nike's official API endpoint.
"""

import pandas as pd
import requests
from src.core.base_scraper import BaseScraper


class NikeStoresScraper(BaseScraper):
    """Scraper for Nike Stores locations"""

    def scrape(self) -> pd.DataFrame:
        """
        Scrape Nike store locations from their official API
        
        Returns:
            pd.DataFrame: Location data with standardized columns
        """
        headers = {
            "authority": "api.nike.com",
            "content-type": "application/json; charset=UTF-8",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        # This API call gets all Nike stores with a large radius
        url = ("https://api.nike.com/store/store_locations/v1?"
               "fields=storeNumber,storeServices,id,name,address,businessConcept,company,coordinates,distance,facilityType,imageUrl,localizations,storeConcept,offerings,operationalDetails,phone,region,slug,timezone,defaultLanguage&"
               "search=(facilityType%20=in=%20(%20%27NIKE_OWNED_STORE%27))%20and%20(businessConcept%20=out=%20(%27EMPLOYEE_STORE%27,%20%27BRAND_POPUP%27))%20and%20(brand%20!=%20%27CONVERSE%27)%20and%20(coordinates=geoProximity=%7B%22latitude%22%3A39%2C%22longitude%22%3A-98%2C%22maxDistance%22%3A50000%2C%22measurementUnits%22%3A%22mi%22%7D)&"
               "count=2000")
        
        response = self.session.get(url, headers=headers, timeout=self.config.get('timeout', 30))
        response.raise_for_status()
        
        data = response.json()
        
        if "objects" not in data:
            raise ValueError("Unexpected API response structure")
        
        stores = data["objects"]
        
        if not stores:
            raise ValueError("No stores found")
        
        # Extract data from each store
        store_list = []
        for store in stores:
            # Get address information
            address_info = store.get("address", {})
            
            # Get coordinates
            coordinates = store.get("coordinates", {})
            
            store_dict = {
                "store_id": store.get("storeNumber"),
                "name": store.get("name"),
                "address": address_info.get("address1"),
                "address2": address_info.get("address2"),
                "city": address_info.get("city"),
                "state": address_info.get("state"),
                "zip_code": address_info.get("postalCode"),
                "phone": store.get("phone"),
                "latitude": coordinates.get("latitude"),
                "longitude": coordinates.get("longitude"),
                "store_type": store.get("businessConcept"),
                "timezone": store.get("timezone")
            }
            store_list.append(store_dict)
        
        df = pd.DataFrame(store_list)
        return df
