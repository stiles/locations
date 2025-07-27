import pandas as pd
import geopandas as gpd
from typing import Dict, Tuple
import logging
import usaddress
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

class DataProcessor:
    """Handles common data processing operations"""
    
    def __init__(self):
        self.geocoder = Nominatim(user_agent="locations-scraper")
        self.geocode_func = RateLimiter(self.geocoder.geocode, min_delay_seconds=1)
        
    def clean_address(self, address: str) -> str:
        """Standardize address format"""
        if pd.isna(address):
            return ""
        # Remove extra whitespace, normalize formatting
        return " ".join(str(address).split())
        
    def parse_address(self, address: str) -> Dict:
        """Parse address into components using usaddress"""
        try:
            parsed = usaddress.tag(address)
            return parsed[0] if parsed else {}
        except:
            return {}
            
    def geocode_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add latitude/longitude to locations missing coordinates"""
        df = df.copy()
        
        for idx, row in df.iterrows():
            if pd.isna(row.get('latitude')) or pd.isna(row.get('longitude')):
                full_address = f"{row['address']}, {row['city']}, {row['state']}"
                try:
                    location = self.geocode_func(full_address)
                    if location:
                        df.loc[idx, 'latitude'] = location.latitude
                        df.loc[idx, 'longitude'] = location.longitude
                        logger.debug(f"Geocoded: {full_address}")
                except Exception as e:
                    logger.warning(f"Geocoding failed for {full_address}: {e}")
                    
        return df
        
    def standardize_columns(self, df: pd.DataFrame, company_name: str) -> pd.DataFrame:
        """Standardize column names and add metadata"""
        df = df.copy()
        
        # Add metadata columns
        df['company'] = company_name
        df['scraped_date'] = pd.Timestamp.now().strftime("%Y-%m-%d")
        
        # Standardize state abbreviations
        if 'state' in df.columns:
            df['state'] = df['state'].str.upper()
        
        # Create unique identifier
        df['location_id'] = df.apply(
            lambda row: f"{company_name}_{hash(str(row.get('address', '')) + '_' + str(row.get('city', '')) + '_' + str(row.get('state', '')))}",
            axis=1
        )
        
        return df
        
    def create_geodataframe(self, df: pd.DataFrame) -> gpd.GeoDataFrame:
        """Convert DataFrame to GeoDataFrame with Point geometries"""
        df_geo = df.dropna(subset=['latitude', 'longitude']).copy()
        if len(df_geo) == 0:
            return gpd.GeoDataFrame(df, crs='EPSG:4326')
        
        return gpd.GeoDataFrame(
            df_geo,
            geometry=gpd.points_from_xy(df_geo.longitude, df_geo.latitude),
            crs='EPSG:4326'
        )
        
    def get_quality_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate data quality metrics"""
        total_locations = len(df)
        if total_locations == 0:
            return {"total_locations": 0}
            
        geocoded = df.dropna(subset=['latitude', 'longitude'])
        
        return {
            "total_locations": total_locations,
            "geocoded_locations": len(geocoded),
            "geocoding_rate": len(geocoded) / total_locations if total_locations > 0 else 0,
            "states_covered": df['state'].nunique() if 'state' in df.columns else 0,
            "missing_addresses": df['address'].isna().sum() if 'address' in df.columns else 0,
            "missing_cities": df['city'].isna().sum() if 'city' in df.columns else 0
        }
        
    def process(self, raw_data: pd.DataFrame, company_name: str) -> pd.DataFrame:
        """Full processing pipeline"""
        logger.info(f"Processing data for {company_name}")
        df = raw_data.copy()
        
        # Clean and standardize
        if 'address' in df.columns:
            df['address'] = df['address'].apply(self.clean_address)
        
        df = self.standardize_columns(df, company_name)
        
        # Geocode missing coordinates
        df = self.geocode_locations(df)
        
        logger.info(f"Processed {len(df)} locations for {company_name}")
        return df 