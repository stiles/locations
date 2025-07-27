import boto3
import pandas as pd
import geopandas as gpd
import os
import json
from io import StringIO, BytesIO
from typing import Dict, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class S3Storage:
    """Handles S3 storage operations for location data"""
    
    def __init__(self, bucket_name: str = None):
        self.bucket_name = bucket_name or os.getenv('LOCATIONS_S3_BUCKET')
        self.s3_prefix = "locations/"  # Base path within bucket
        if not self.bucket_name:
            logger.warning("S3 bucket not configured, using local storage fallback")
            self.use_local = True
        else:
            self.use_local = False
            try:
                # Use specific AWS profile
                session = boto3.Session(profile_name='haekeo')
                self.s3_client = session.client('s3')
            except Exception as e:
                logger.warning(f"S3 client initialization failed: {e}, using local storage")
                self.use_local = True
        
    def save_processed_data(self, df: pd.DataFrame, company: str, timestamp: str) -> Dict[str, str]:
        """Save processed data in multiple formats"""
        if self.use_local:
            return self._save_local_processed_data(df, company, timestamp)
            
        urls = {}
        base_path = f"{self.s3_prefix}processed/{company}/{timestamp}"
        
        # Save CSV
        csv_key = f"{base_path}/locations.csv"
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=csv_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        urls['csv'] = f"s3://{self.bucket_name}/{csv_key}"
        
        # Save GeoJSON (only for geocoded data)
        geocoded_df = df.dropna(subset=['latitude', 'longitude'])
        if len(geocoded_df) > 0:
            gdf = gpd.GeoDataFrame(
                geocoded_df,
                geometry=gpd.points_from_xy(geocoded_df.longitude, geocoded_df.latitude),
                crs='EPSG:4326'
            )
            geojson_key = f"{base_path}/locations.geojson"
            geojson_buffer = BytesIO()
            gdf.to_file(geojson_buffer, driver='GeoJSON')
            geojson_buffer.seek(0)  # Reset buffer position
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=geojson_key,
                Body=geojson_buffer.getvalue(),
                ContentType='application/geo+json'
            )
            urls['geojson'] = f"s3://{self.bucket_name}/{geojson_key}"
        
        # Save JSON
        json_key = f"{base_path}/locations.json"
        json_data = df.to_json(orient='records', indent=2)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=json_key,
            Body=json_data,
            ContentType='application/json'
        )
        urls['json'] = f"s3://{self.bucket_name}/{json_key}"
        
        # Save metadata
        metadata = {
            "company": company,
            "timestamp": timestamp,
            "record_count": int(len(df)),
            "geocoded_count": int(len(geocoded_df)),
            "columns": list(df.columns),
            "data_quality": self._calculate_quality_metrics(df)
        }
        metadata_key = f"{base_path}/metadata.json"
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2, default=str),
            ContentType='application/json'
        )
        urls['metadata'] = f"s3://{self.bucket_name}/{metadata_key}"
        
        # Update latest symlinks (placeholder for now)
        # self._update_latest_links(company, timestamp)
        
        return urls
    
    def _save_local_processed_data(self, df: pd.DataFrame, company: str, timestamp: str) -> Dict[str, str]:
        """Fallback to local storage when S3 is not available"""
        base_dir = f"data/processed/{company}/{timestamp}"
        os.makedirs(base_dir, exist_ok=True)
        
        urls = {}
        
        # Save CSV
        csv_path = f"{base_dir}/locations.csv"
        df.to_csv(csv_path, index=False)
        urls['csv'] = csv_path
        
        # Save GeoJSON (only for geocoded data)
        geocoded_df = df.dropna(subset=['latitude', 'longitude'])
        if len(geocoded_df) > 0:
            gdf = gpd.GeoDataFrame(
                geocoded_df,
                geometry=gpd.points_from_xy(geocoded_df.longitude, geocoded_df.latitude),
                crs='EPSG:4326'
            )
            geojson_path = f"{base_dir}/locations.geojson"
            gdf.to_file(geojson_path, driver='GeoJSON')
            urls['geojson'] = geojson_path
        
        # Save JSON
        json_path = f"{base_dir}/locations.json"
        df.to_json(json_path, orient='records', indent=2)
        urls['json'] = json_path
        
        # Save metadata
        metadata = {
            "company": company,
            "timestamp": timestamp,
            "record_count": int(len(df)),
            "geocoded_count": int(len(geocoded_df)),
            "columns": list(df.columns),
            "data_quality": self._calculate_quality_metrics(df)
        }
        metadata_path = f"{base_dir}/metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        urls['metadata'] = metadata_path
        
        return urls
        
    def save_raw_data(self, data: pd.DataFrame, company: str, timestamp: str):
        """Save raw scraped data for debugging"""
        if self.use_local:
            base_dir = f"data/raw/{company}/{timestamp}"
            os.makedirs(base_dir, exist_ok=True)
            raw_path = f"{base_dir}/raw_data.json"
            data.to_json(raw_path, orient='records', indent=2)
            return raw_path
        else:
            raw_key = f"{self.s3_prefix}raw/{company}/{timestamp}/raw_data.json"
            raw_json = data.to_json(orient='records', indent=2)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=raw_key,
                Body=raw_json,
                ContentType='application/json'
            )
            return f"s3://{self.bucket_name}/{raw_key}"
        
    def get_latest_data(self, company: str) -> pd.DataFrame:
        """Retrieve latest processed data for a company"""
        try:
            if self.use_local:
                # For local storage, find the most recent timestamp directory
                company_dir = f"data/processed/{company}"
                if not os.path.exists(company_dir):
                    return pd.DataFrame()
                
                timestamps = os.listdir(company_dir)
                if not timestamps:
                    return pd.DataFrame()
                
                latest_timestamp = max(timestamps)
                csv_path = f"{company_dir}/{latest_timestamp}/locations.csv"
                if os.path.exists(csv_path):
                    return pd.read_csv(csv_path)
            else:
                key = f"{self.s3_prefix}processed/{company}/latest/locations.csv"
                obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
                return pd.read_csv(obj['Body'])
        except Exception as e:
            logger.error(f"Failed to retrieve latest data for {company}: {e}")
            
        return pd.DataFrame()
        
    def _calculate_quality_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate data quality metrics"""
        total_locations = len(df)
        if total_locations == 0:
            return {"total_locations": 0}
            
        geocoded = df.dropna(subset=['latitude', 'longitude'])
        
        return {
            "total_locations": int(total_locations),
            "geocoded_locations": int(len(geocoded)),
            "geocoding_rate": float(len(geocoded) / total_locations if total_locations > 0 else 0),
            "states_covered": int(df['state'].nunique() if 'state' in df.columns else 0),
            "missing_addresses": int(df['address'].isna().sum() if 'address' in df.columns else 0),
            "missing_cities": int(df['city'].isna().sum() if 'city' in df.columns else 0)
        } 