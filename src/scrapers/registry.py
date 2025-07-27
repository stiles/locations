import importlib
import logging
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from src.core.base_scraper import BaseScraper
from src.utils.config import config

logger = logging.getLogger(__name__)


class ScraperRegistry:
    """Registry for auto-discovery and execution of scrapers"""
    
    def __init__(self):
        self.scrapers = {}
        self._discover_scrapers()
    
    def _discover_scrapers(self):
        """Auto-discover available scraper classes"""
        # Get all configured companies
        companies = config.get_all_company_names()
        
        for company_name in companies:
            company_config = config.get_company_config(company_name)
            scraper_class_name = company_config.get('scraper_class')
            
            if scraper_class_name:
                try:
                    # Try to import the scraper module
                    module_name = f"src.scrapers.{company_name.replace('-', '_')}"
                    module = importlib.import_module(module_name)
                    
                    # Get the scraper class
                    scraper_class = getattr(module, scraper_class_name)
                    
                    # Verify it's a BaseScraper subclass
                    if issubclass(scraper_class, BaseScraper):
                        self.scrapers[company_name] = scraper_class
                        logger.debug(f"Registered scraper: {company_name} -> {scraper_class_name}")
                    else:
                        logger.warning(f"Scraper {scraper_class_name} is not a BaseScraper subclass")
                        
                except ImportError:
                    logger.debug(f"Scraper module not found for {company_name}")
                except AttributeError:
                    logger.warning(f"Scraper class {scraper_class_name} not found in module")
                except Exception as e:
                    logger.error(f"Error loading scraper {company_name}: {e}")
        
        logger.info(f"Discovered {len(self.scrapers)} scrapers")
    
    def get_scraper(self, company_name: str) -> BaseScraper:
        """Get a scraper instance for a company"""
        if company_name not in self.scrapers:
            raise ValueError(f"No scraper found for company: {company_name}")
        
        company_config = config.get_company_config(company_name)
        scraper_class = self.scrapers[company_name]
        
        return scraper_class(company_name, company_config)
    
    def run_single(self, company_name: str) -> Dict[str, Any]:
        """Run a single scraper"""
        try:
            scraper = self.get_scraper(company_name)
            logger.info(f"Starting scraper for {company_name}")
            start_time = time.time()
            
            result = scraper.run()
            
            elapsed_time = time.time() - start_time
            result['elapsed_time'] = elapsed_time
            
            logger.info(f"Completed {company_name} in {elapsed_time:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Error running scraper for {company_name}: {e}")
            return {
                "company": company_name,
                "status": "error",
                "error": str(e),
                "timestamp": time.strftime("%Y-%m-%d")
            }
    
    def run_sequential(self, company_names: List[str], companies_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Run scrapers sequentially"""
        results = []
        
        for i, company_name in enumerate(company_names, 1):
            logger.info(f"Processing {company_name} ({i}/{len(company_names)})")
            
            # Check if scraper exists
            if company_name not in self.scrapers:
                logger.warning(f"Scraper not implemented for {company_name}")
                results.append({
                    "company": company_name,
                    "status": "error", 
                    "error": "Scraper not implemented",
                    "timestamp": time.strftime("%Y-%m-%d")
                })
                continue
            
            result = self.run_single(company_name)
            results.append(result)
            
            # Rate limiting between requests
            if i < len(company_names):  # Don't sleep after the last one
                company_config = companies_config.get("companies", {}).get(company_name, {})
                rate_limit = company_config.get('rate_limit', 1.0)
                time.sleep(rate_limit)
        
        return results
    
    def run_parallel(self, company_names: List[str], companies_config: Dict[str, Any], max_workers: int = 3) -> List[Dict[str, Any]]:
        """Run scrapers in parallel with rate limiting"""
        results = []
        
        # Filter to only companies with implemented scrapers
        available_companies = [name for name in company_names if name in self.scrapers]
        unavailable_companies = [name for name in company_names if name not in self.scrapers]
        
        # Add error results for unavailable scrapers
        for company_name in unavailable_companies:
            logger.warning(f"Scraper not implemented for {company_name}")
            results.append({
                "company": company_name,
                "status": "error",
                "error": "Scraper not implemented", 
                "timestamp": time.strftime("%Y-%m-%d")
            })
        
        # Run available scrapers in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_company = {
                executor.submit(self.run_single, company_name): company_name 
                for company_name in available_companies
            }
            
            for future in as_completed(future_to_company):
                company_name = future_to_company[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Parallel execution failed for {company_name}: {e}")
                    results.append({
                        "company": company_name,
                        "status": "error",
                        "error": str(e),
                        "timestamp": time.strftime("%Y-%m-%d")
                    })
        
        return results
    
    def list_available_scrapers(self) -> List[str]:
        """Get list of available scraper names"""
        return list(self.scrapers.keys())
    
    def refresh_registry(self):
        """Refresh the scraper registry (useful during development)"""
        self.scrapers.clear()
        self._discover_scrapers() 