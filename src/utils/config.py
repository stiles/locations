import yaml
import os
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class Config:
    """Configuration management for the location scraping system"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = config_dir
        self._companies = None
        self._settings = None
        
    def load_companies(self) -> Dict[str, Any]:
        """Load company configurations from YAML"""
        if self._companies is None:
            companies_path = os.path.join(self.config_dir, "companies.yaml")
            try:
                with open(companies_path, 'r') as f:
                    self._companies = yaml.safe_load(f)
                logger.info(f"Loaded {len(self._companies.get('companies', {}))} company configurations")
            except FileNotFoundError:
                logger.error(f"Companies config file not found: {companies_path}")
                self._companies = {"companies": {}, "categories": {}}
            except yaml.YAMLError as e:
                logger.error(f"Error parsing companies YAML: {e}")
                self._companies = {"companies": {}, "categories": {}}
                
        return self._companies
        
    def load_settings(self) -> Dict[str, Any]:
        """Load global settings from YAML"""
        if self._settings is None:
            settings_path = os.path.join(self.config_dir, "settings.yaml")
            try:
                with open(settings_path, 'r') as f:
                    self._settings = yaml.safe_load(f)
                logger.info("Loaded global settings")
            except FileNotFoundError:
                logger.error(f"Settings config file not found: {settings_path}")
                self._settings = self._get_default_settings()
            except yaml.YAMLError as e:
                logger.error(f"Error parsing settings YAML: {e}")
                self._settings = self._get_default_settings()
                
        return self._settings
        
    def get_company_config(self, company_name: str) -> Dict[str, Any]:
        """Get configuration for a specific company"""
        companies = self.load_companies()
        company_config = companies.get("companies", {}).get(company_name, {})
        
        if not company_config:
            logger.warning(f"No configuration found for company: {company_name}")
            
        return company_config
        
    def get_companies_in_category(self, category: str) -> list:
        """Get list of companies in a specific category"""
        companies = self.load_companies()
        return companies.get("categories", {}).get(category, [])
        
    def get_all_company_names(self) -> list:
        """Get list of all configured company names"""
        companies = self.load_companies()
        return list(companies.get("companies", {}).keys())
        
    def get_setting(self, key_path: str, default=None):
        """Get a setting value using dot notation (e.g., 'storage.s3_bucket')"""
        settings = self.load_settings()
        keys = key_path.split('.')
        
        value = settings
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
                
        return value
        
    def _get_default_settings(self) -> Dict[str, Any]:
        """Return default settings if config file is missing"""
        return {
            "storage": {
                "s3_bucket": "",
                "local_cache_dir": "data/cache",
                "retention_days": 90
            },
            "geocoding": {
                "service": "nominatim",
                "rate_limit": 1.0,
                "timeout": 10
            },
            "data_processing": {
                "required_columns": ["name", "address", "city", "state"],
                "geocoding_threshold": 0.8,
                "max_locations_per_company": 50000
            },
            "logging": {
                "level": "INFO",
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "file": "logs/locations.log"
            }
        }


# Global config instance
config = Config() 