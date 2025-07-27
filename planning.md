# Locations refactor plan

> **🚀 PROJECT STATUS (Updated 2025-01-27)**
> 
> - **PHASE 1 COMPLETE**: Core architecture built and proven ✅
> - **CURRENT**: Ready to start Phase 2 (Migration) 🎯  
> - **NEXT STEPS**: Convert 20-25 simple scrapers in Week 3
> - **INFRASTRUCTURE**: S3 storage, uv development environment, CLI tools ready
> - **PROVEN**: 1,046 locations scraped successfully (In-N-Out + Trader Joe's)
> 
> **🛠️ Quick Start for Phase 2**:
> ```bash
> git checkout modernization    # Switch to development branch
> make setup                   # Install dependencies
> make dry-run                 # Test current scrapers  
> make scrape COMPANY=in-n-out # Verify working
> ```

## Executive summary

The current locations repository has grown to 76 companies with 202,664 locations across Jupyter notebooks. While functional, it faces maintainability challenges due to code duplication, manual processes and local data storage. This plan outlines a refactor to improve automation, maintainability and scalability.

## Current state analysis

### Strengths
- Comprehensive dataset (76 companies, 202k+ locations)
- Consistent data output formats (CSV, GeoJSON, JSON)
- Working scraping methodology
- Real-world impact (CNN publications)

### Pain points
- **Code Duplication**: 76+ notebooks with shared setup/processing code
- **Manual Maintenance**: Hand-curated README tables and statistics
- **Storage Issues**: Large repository size due to local data storage
- **No Automation**: Manual execution and updates
- **Inconsistent Naming**: Mixed file naming conventions
- **Scalability Limits**: Difficult to add new companies or bulk updates

### Shared code patterns identified
- Standard imports: `pandas`, `geopandas`, `requests`, `BeautifulSoup`, `tqdm`
- Common setup: pandas display options, `USE_PYGEOS` environment variable  
- Consistent export pattern: CSV and GeoJSON with standardized naming
- Similar workflow: fetch → process → geocode → export

## Proposed architecture

### Directory structure
```
locations/
├── src/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── base_scraper.py      # Abstract base class for all scrapers
│   │   ├── data_processor.py    # Common data processing utilities
│   │   ├── storage.py           # S3/cloud storage interface
│   │   ├── geocoding.py         # Geocoding utilities
│   │   └── exceptions.py        # Custom exception classes
│   ├── scrapers/
│   │   ├── __init__.py
│   │   ├── registry.py          # Auto-discovery of scraper classes
│   │   ├── mcdonalds.py         # Individual scraper implementations
│   │   ├── starbucks.py
│   │   ├── chipotle.py
│   │   └── ...                  # One file per company
│   └── utils/
│       ├── __init__.py
│       ├── config.py            # Configuration management
│       ├── logging.py           # Centralized logging
│       ├── validation.py        # Data validation utilities
│       └── helpers.py           # Shared utility functions
├── config/
│   ├── companies.yaml           # Company metadata and configurations
│   ├── settings.yaml            # Global application settings
│   └── categories.yaml          # Business category definitions
├── scripts/
│   ├── run_scraper.py           # CLI interface for running scrapers
│   ├── run_all.py               # Batch processing script
│   ├── generate_readme.py       # Auto-generate README from data
│   ├── migrate_data.py          # Migration utilities for existing data
│   ├── validate_data.py         # Data quality checks
│   └── deploy.py                # Deployment utilities
├── templates/
│   ├── readme_template.md       # Jinja2 template for README generation
│   └── company_template.py      # Template for new scrapers
├── tests/
│   ├── __init__.py
│   ├── test_scrapers/
│   ├── test_data_processor.py
│   └── test_storage.py
├── .github/
│   └── workflows/
│       ├── scrape_schedule.yml  # Automated scraping
│       ├── update_readme.yml    # Auto-update README
│       ├── data_validation.yml  # Data quality checks
│       └── test.yml             # Testing pipeline
├── docs/
│   ├── adding_companies.md      # Guide for adding new companies
│   ├── architecture.md          # Technical architecture documentation
│   └── troubleshooting.md       # Common issues and solutions
├── data/                        # Local cache only (gitignored)
│   └── cache/
├── notebooks/                   # Analysis notebooks (not scrapers)
│   ├── exploratory/
│   └── analysis/
├── requirements.txt
├── setup.py
├── pyproject.toml
└── README.md                    # Auto-generated
```

### Cloud storage strategy

#### S3 Bucket Structure
```
s3://your-locations-bucket/
├── processed/                   # Clean, final data
│   ├── mcdonalds/
│   │   ├── 2024-12-20/
│   │   │   ├── locations.csv
│   │   │   ├── locations.geojson
│   │   │   ├── locations.json
│   │   │   └── metadata.json
│   │   ├── 2024-12-13/
│   │   └── latest/              # Symlinks to most recent
│   │       ├── locations.csv -> ../2024-12-20/locations.csv
│   │       └── ...
│   └── starbucks/
│       └── ...
├── raw/                         # Raw scraped data for debugging
│   ├── mcdonalds/
│   │   └── 2024-12-20/
│   │       └── raw_response.json
│   └── ...
├── aggregated/                  # Cross-company analysis files
│   ├── 2024-12-20/
│   │   ├── all_locations.parquet
│   │   ├── summary_stats.json
│   │   └── category_breakdown.json
│   └── latest/
└── backups/                     # Historical snapshots
    ├── 2024-q4/
    └── ...
```

#### Data versioning strategy
- **Timestamps**: YYYY-MM-DD format for daily snapshots
- **Latest Links**: Symbolic links pointing to most recent data
- **Retention Policy**: Keep daily snapshots for 90 days, weekly for 1 year, monthly forever
- **Change Detection**: Compare new data with previous version to detect significant changes

## Core components

### Base scraper class

```python
# src/core/base_scraper.py
from abc import ABC, abstractmethod
import pandas as pd
import geopandas as gpd
from datetime import datetime
from typing import Dict, Optional, List
from .storage import S3Storage
from .data_processor import DataProcessor
from .exceptions import ScrapingError, ValidationError

class BaseScraper(ABC):
    """Abstract base class for all location scrapers"""
    
    def __init__(self, company_name: str, config: dict):
        self.company_name = company_name
        self.config = config
        self.storage = S3Storage()
        self.processor = DataProcessor()
        self.timestamp = datetime.now().strftime("%Y-%m-%d")
        self.session = requests.Session()
        self._setup_session()
        
    def _setup_session(self):
        """Configure requests session with headers and rate limiting"""
        self.session.headers.update(self.config.get('headers', {}))
        
    @abstractmethod
    def scrape(self) -> pd.DataFrame:
        """Implement company-specific scraping logic
        
        Returns:
            pd.DataFrame: Raw location data with at minimum:
                - name: Location name
                - address: Full address
                - city: City name
                - state: State abbreviation
                - zip_code: ZIP code
                - latitude: Latitude (if available)
                - longitude: Longitude (if available)
        """
        pass
    
    def validate_data(self, data: pd.DataFrame) -> None:
        """Validate scraped data meets minimum requirements"""
        required_columns = ['name', 'address', 'city', 'state']
        missing_columns = set(required_columns) - set(data.columns)
        if missing_columns:
            raise ValidationError(f"Missing required columns: {missing_columns}")
            
        if len(data) == 0:
            raise ValidationError("No locations found")
            
    def run(self) -> Dict:
        """Standard workflow: scrape → validate → process → store → return summary"""
        try:
            # Scrape raw data
            raw_data = self.scrape()
            
            # Validate data
            self.validate_data(raw_data)
            
            # Store raw data for debugging
            self.storage.save_raw_data(raw_data, self.company_name, self.timestamp)
            
            # Process data (clean, geocode, standardize)
            processed_data = self.processor.process(raw_data, self.company_name)
            
            # Store processed data to S3
            urls = self.storage.save_processed_data(
                processed_data, self.company_name, self.timestamp
            )
            
            # Return summary
            return {
                "company": self.company_name,
                "locations_count": len(processed_data),
                "timestamp": self.timestamp,
                "files": urls,
                "status": "success",
                "data_quality": self.processor.get_quality_metrics(processed_data)
            }
            
        except Exception as e:
            logger.error(f"Error scraping {self.company_name}: {str(e)}")
            return {
                "company": self.company_name,
                "status": "error",
                "error": str(e),
                "timestamp": self.timestamp
            }
```

### Data Processor

```python
# src/core/data_processor.py
import pandas as pd
import geopandas as gpd
from typing import Dict, Tuple
import usaddress
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

class DataProcessor:
    """Handles common data processing operations"""
    
    def __init__(self):
        self.geocoder = Nominatim(user_agent="locations-scraper")
        self.geocode_func = RateLimiter(self.geocoder.geocode, min_delay_seconds=1)
        
    def clean_address(self, address: str) -> str:
        """Standardize address format"""
        # Remove extra whitespace, normalize formatting
        return " ".join(address.split())
        
    def parse_address(self, address: str) -> Dict:
        """Parse address into components using usaddress"""
        try:
            parsed = usaddress.tag(address)
            return parsed[0] if parsed else {}
        except:
            return {}
            
    def geocode_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add latitude/longitude to locations missing coordinates"""
        for idx, row in df.iterrows():
            if pd.isna(row.get('latitude')) or pd.isna(row.get('longitude')):
                full_address = f"{row['address']}, {row['city']}, {row['state']}"
                try:
                    location = self.geocode_func(full_address)
                    if location:
                        df.loc[idx, 'latitude'] = location.latitude
                        df.loc[idx, 'longitude'] = location.longitude
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
        df['state'] = df['state'].str.upper()
        
        # Create unique identifier
        df['location_id'] = df.apply(
            lambda row: f"{company_name}_{hash(f'{row.address}_{row.city}_{row.state}')}", 
            axis=1
        )
        
        return df
        
    def create_geodataframe(self, df: pd.DataFrame) -> gpd.GeoDataFrame:
        """Convert DataFrame to GeoDataFrame with Point geometries"""
        df_geo = df.dropna(subset=['latitude', 'longitude']).copy()
        return gpd.GeoDataFrame(
            df_geo, 
            geometry=gpd.points_from_xy(df_geo.longitude, df_geo.latitude),
            crs='EPSG:4326'
        )
        
    def get_quality_metrics(self, df: pd.DataFrame) -> Dict:
        """Calculate data quality metrics"""
        total_locations = len(df)
        geocoded = df.dropna(subset=['latitude', 'longitude'])
        
        return {
            "total_locations": total_locations,
            "geocoded_locations": len(geocoded),
            "geocoding_rate": len(geocoded) / total_locations if total_locations > 0 else 0,
            "states_covered": df['state'].nunique(),
            "missing_addresses": df['address'].isna().sum(),
            "missing_cities": df['city'].isna().sum()
        }
        
    def process(self, raw_data: pd.DataFrame, company_name: str) -> pd.DataFrame:
        """Full processing pipeline"""
        df = raw_data.copy()
        
        # Clean and standardize
        df['address'] = df['address'].apply(self.clean_address)
        df = self.standardize_columns(df, company_name)
        
        # Geocode missing coordinates
        df = self.geocode_locations(df)
        
        return df
```

### Storage interface

```python
# src/core/storage.py
import boto3
import pandas as pd
import geopandas as gpd
from pathlib import Path
import json
from typing import Dict, List
from datetime import datetime

class S3Storage:
    """Handles S3 storage operations for location data"""
    
    def __init__(self, bucket_name: str = None):
        self.bucket_name = bucket_name or os.getenv('LOCATIONS_S3_BUCKET')
        self.s3_client = boto3.client('s3')
        
    def save_processed_data(self, df: pd.DataFrame, company: str, timestamp: str) -> Dict[str, str]:
        """Save processed data in multiple formats"""
        urls = {}
        base_path = f"processed/{company}/{timestamp}"
        
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
        
        # Save GeoJSON
        gdf = gpd.GeoDataFrame(
            df.dropna(subset=['latitude', 'longitude']),
            geometry=gpd.points_from_xy(df.longitude, df.latitude),
            crs='EPSG:4326'
        )
        geojson_key = f"{base_path}/locations.geojson"
        geojson_buffer = StringIO()
        gdf.to_file(geojson_buffer, driver='GeoJSON')
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
            "record_count": len(df),
            "geocoded_count": len(gdf),
            "columns": list(df.columns),
            "data_quality": self._calculate_quality_metrics(df)
        }
        metadata_key = f"{base_path}/metadata.json"
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json'
        )
        urls['metadata'] = f"s3://{self.bucket_name}/{metadata_key}"
        
        # Update latest symlinks
        self._update_latest_links(company, timestamp)
        
        return urls
        
    def save_raw_data(self, data: pd.DataFrame, company: str, timestamp: str):
        """Save raw scraped data for debugging"""
        raw_key = f"raw/{company}/{timestamp}/raw_data.json"
        raw_json = data.to_json(orient='records', indent=2)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=raw_key,
            Body=raw_json,
            ContentType='application/json'
        )
        
    def get_latest_data(self, company: str) -> pd.DataFrame:
        """Retrieve latest processed data for a company"""
        try:
            key = f"processed/{company}/latest/locations.csv"
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return pd.read_csv(obj['Body'])
        except Exception as e:
            logger.error(f"Failed to retrieve latest data for {company}: {e}")
            return pd.DataFrame()
            
    def get_summary_stats(self) -> Dict:
        """Generate summary statistics across all companies"""
        # Implementation for aggregating data across all companies
        pass
```

### Configuration Management

```yaml
# config/companies.yaml
companies:
  mcdonalds:
    name: "McDonald's"
    category: "Fast Food & Quick Service"
    scraper_class: "McdonaldsScraper"
    base_url: "https://www.mcdonalds.com/us/en-us/restaurant-locator.html"
    rate_limit: 1.0  # seconds between requests
    timeout: 30
    headers:
      User-Agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    retry_attempts: 3
    
  starbucks:
    name: "Starbucks"
    category: "Coffee Shops & Desserts"
    scraper_class: "StarbucksScraper"
    api_endpoint: "https://store-locator.starbucks.com/api/locations"
    rate_limit: 0.5
    timeout: 45
    headers:
      User-Agent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
      Accept: "application/json"
    pagination:
      enabled: true
      page_size: 100
      
  # ... continue for all 76 companies
```

```yaml
# config/settings.yaml
storage:
  s3_bucket: "your-locations-bucket"
  local_cache_dir: "data/cache"
  retention_days: 90

geocoding:
  service: "nominatim"
  rate_limit: 1.0
  timeout: 10
  fallback_service: "google"  # if configured

data_processing:
  required_columns: ["name", "address", "city", "state"]
  geocoding_threshold: 0.8  # minimum geocoding success rate
  max_locations_per_company: 50000

logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file: "logs/locations.log"

automation:
  schedule: "weekly"  # daily, weekly, monthly
  error_threshold: 0.1  # fail if >10% of companies fail
  notification_email: "your-email@example.com"
```

## CLI interface

```python
# scripts/run_scraper.py
import click
import yaml
from src.scrapers.registry import ScraperRegistry
from src.utils.logging import setup_logging

@click.command()
@click.option('--company', help='Company name to scrape')
@click.option('--all', 'scrape_all', is_flag=True, help='Scrape all companies')
@click.option('--category', help='Scrape all companies in category')
@click.option('--config', default='config/companies.yaml', help='Config file path')
@click.option('--dry-run', is_flag=True, help='Validate without running')
@click.option('--parallel', is_flag=True, help='Run multiple scrapers in parallel')
def main(company, scrape_all, category, config, dry_run, parallel):
    """Location scraper CLI"""
    setup_logging()
    
    with open(config, 'r') as f:
        companies_config = yaml.safe_load(f)
    
    registry = ScraperRegistry()
    
    if scrape_all:
        companies_to_run = list(companies_config['companies'].keys())
    elif category:
        companies_to_run = [
            name for name, config in companies_config['companies'].items()
            if config.get('category') == category
        ]
    elif company:
        companies_to_run = [company]
    else:
        click.echo("Specify --company, --category, or --all")
        return
    
    if dry_run:
        click.echo(f"Would scrape: {', '.join(companies_to_run)}")
        return
        
    if parallel:
        results = registry.run_parallel(companies_to_run, companies_config)
    else:
        results = registry.run_sequential(companies_to_run, companies_config)
    
    # Print summary
    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'error']
    
    click.echo(f"\nCompleted: {len(successful)} successful, {len(failed)} failed")
    
    if failed:
        click.echo("\nFailed companies:")
        for result in failed:
            click.echo(f"  {result['company']}: {result['error']}")

if __name__ == '__main__':
    main()
```

## GitHub Actions automation

```yaml
# .github/workflows/scrape_schedule.yml
name: Scheduled Location Scraping

on:
  schedule:
    - cron: '0 6 * * 0'  # Weekly on Sunday at 6 AM UTC
  workflow_dispatch:     # Manual trigger
    inputs:
      companies:
        description: 'Comma-separated list of companies (leave empty for all)'
        required: false
        type: string
      category:
        description: 'Scrape specific category'
        required: false
        type: choice
        options:
          - ''
          - 'Fast Food & Quick Service'
          - 'Coffee Shops & Desserts'
          - 'Retail Stores'
          - 'Casual Dining'

jobs:
  scrape:
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        batch: [1, 2, 3, 4]  # Split companies into 4 batches for parallel processing
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
          cache: 'pip'
          
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
          
      - name: Run scrapers
        run: |
          if [ "${{ github.event.inputs.companies }}" != "" ]; then
            python scripts/run_scraper.py --company ${{ github.event.inputs.companies }}
          elif [ "${{ github.event.inputs.category }}" != "" ]; then
            python scripts/run_scraper.py --category "${{ github.event.inputs.category }}"
          else
            python scripts/run_scraper.py --batch ${{ matrix.batch }} --parallel
          fi
          
      - name: Upload logs
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: scraping-logs-batch-${{ matrix.batch }}
          path: logs/
          
  update-readme:
    needs: scrape
    runs-on: ubuntu-latest
    if: always()
    
    steps:
      - name: Checkout code
        uses: actions/checkout@v4
        
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'
          
      - name: Install dependencies
        run: pip install -r requirements.txt
        
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
          
      - name: Generate README
        run: python scripts/generate_readme.py
        
      - name: Commit and push changes
        run: |
          git config --local user.email "action@github.com"
          git config --local user.name "GitHub Action"
          git add README.md
          if git diff --staged --quiet; then
            echo "No changes to commit"
          else
            git commit -m "Auto-update README with latest location data"
            git push
          fi

  notify:
    needs: [scrape, update-readme]
    runs-on: ubuntu-latest
    if: always()
    
    steps:
      - name: Send notification
        if: contains(needs.*.result, 'failure')
        uses: 8398a7/action-slack@v3
        with:
          status: ${{ job.status }}
          channel: '#data-alerts'
          webhook_url: ${{ secrets.SLACK_WEBHOOK }}
```

## Repository strategy

### Approach: Development Branch in Existing Repo (Recommended)

**Preserve Stars, History, and Social Proof:**
- Keep existing repository with all stars, watchers, and commit history
- Create long-running `modernization` development branch
- Build new architecture alongside existing notebooks
- Merge to main when ready for cutover

**Alternative Considered:**
- Separate `locations-v2` development repo
- Rejected due to loss of development history and complexity

### Branch Strategy
```bash
main                  # Current stable notebooks
├── modernization     # New architecture development
    ├── src/          # New modular code
    ├── config/       # YAML configurations  
    ├── scripts/      # CLI tools
    └── notebooks-archive/  # Old notebooks (after cutover)
```

**Benefits:**
- ✅ Preserves repo social proof and history
- ✅ Stable main branch during development
- ✅ Clean development environment
- ✅ Easy rollback if needed
- ✅ Marketing opportunity ("major modernization")

## Implementation Phases

> **📊 CURRENT STATUS (2025-01-27)**: Phase 1 COMPLETE ✅  
> **🎯 NEXT UP**: Phase 2: Migration (Weeks 3-5)  
> **🏗️ INFRASTRUCTURE**: Ready for scale - core architecture proven  
> **📦 ENVIRONMENT**: Modern uv-based development workflow established  
> **☁️ STORAGE**: S3 pipeline working (`s3://stilesdata.com/locations/`)  
> **🧪 TESTING**: 1,046 locations successfully processed through full pipeline

### Phase 1: Foundation (Weeks 1-2) ✅ **COMPLETED 2025-01-27**
**Goal**: Establish core infrastructure and prove the concept

#### Week 1: Core Infrastructure ✅ **COMPLETED**
- [x] Create `modernization` development branch
- [x] Create `src/` directory structure  
- [x] Implement `BaseScraper` abstract class
- [x] Create `DataProcessor` class with geocoding
- [x] Set up S3 storage interface
- [x] Create configuration system (YAML files)
- [x] Set up logging infrastructure
- [x] Create CLI interface (`run_scraper.py`)

#### Week 2: Proof of Concept ✅ **COMPLETED**
- [x] Convert 3 simple scrapers from scratch (In-N-Out ✅, Trader Joe's ✅, Starbucks ⚠️ blocked)
- [x] Test full pipeline: scrape → process → store → validate
- [x] Set up S3 bucket and test data storage (`s3://stilesdata.com/locations/`)
- [x] Create notebook conversion script (not README generation yet)
- [x] Write initial tests
- [x] Tag as `v2.0-alpha`

#### **BONUS COMPLETIONS (Beyond Original Plan)**:
- [x] Modern uv package management with pyproject.toml
- [x] Comprehensive Makefile with 20+ development commands
- [x] DEVELOPMENT.md documentation
- [x] Automated setup script
- [x] S3 storage configuration fixes
- [x] ScraperRegistry with auto-discovery and parallel execution

**Deliverables** ✅:
- ✅ Working core architecture on `modernization` branch
- ✅ 2/3 scrapers working (In-N-Out: 423 locations, Trader Joe's: 623 locations)
- ✅ S3 storage working correctly
- ✅ CLI interface fully functional
- 🎯 **TOTAL: 1,046 locations successfully scraped and stored**

### Phase 2: Migration (Weeks 3-5) 🎯 **READY TO START**
**Goal**: Convert all existing notebook logic to new architecture

#### Week 3: Batch Conversion Strategy 📋 **NEXT PHASE**
- [x] Create conversion script to help migrate notebook logic to Python classes (`scripts/convert_notebooks.py`)
- [x] Set up scraper registry for auto-discovery (`src/scrapers/registry.py`)
- [x] Implement parallel processing (working)
- [ ] **TODO**: Convert 20-25 simpler scrapers (focus on straightforward API/web scraping)
- [ ] **TODO**: Each conversion: extract `scrape()` logic from notebook → implement in new class

**🚀 CONVERSION METHODOLOGY PROVEN**:
- ✅ In-N-Out: Simple JSON API (423 locations)
- ✅ Trader Joe's: POST API with JSON payload (623 locations)  
- ⚠️ Starbucks: Complex ZIP iteration (blocked by anti-bot, but code complete)

#### Week 4: Complex Scrapers 📋 **PLANNED**
- [ ] Convert remaining 50+ scrapers
- [ ] Handle special cases (pagination, authentication, complex APIs)
- [x] Add error handling and retry logic (implemented in BaseScraper)
- [x] Implement data validation rules (implemented in DataProcessor)
- [ ] **TODO**: Validate each conversion against existing data for consistency

#### Week 5: Automation Setup 📋 **PLANNED**
- [ ] Create GitHub Actions workflows
- [ ] Set up scheduled scraping
- [ ] Implement error notifications  
- [ ] Create monitoring dashboard
- [ ] Tag as `v2.0-beta`

**Deliverables** 🎯:
- All 76 scrapers converted to new architecture
- GitHub Actions automation working
- Error handling and monitoring in place ✅ (already implemented)
- Validation that new scrapers produce equivalent data

**INFRASTRUCTURE READY** ✅:
- Core architecture proven and scalable
- Error handling and logging comprehensive  
- S3 storage pipeline working
- Development environment optimized

### Phase 3: Data Management (Weeks 6-7)
**Goal**: Migrate existing data and enhance the system

#### Week 6: Data Migration & Cutover Preparation
- [ ] Migrate all existing CSV/GeoJSON files to S3 with proper timestamps
- [ ] Implement data versioning and retention policies
- [ ] Create aggregated datasets
- [ ] Move old notebooks to `notebooks-archive/` folder
- [ ] Update main README with new architecture documentation
- [ ] Create migration documentation and release notes

#### Week 7: Enhancement & Polish
- [ ] Auto-generated README with templates working end-to-end
- [ ] Data quality monitoring and alerting
- [ ] Performance optimization
- [ ] Complete documentation (adding companies guide, troubleshooting)
- [ ] Final testing of automation workflows

**Deliverables**:
- All historical data migrated to S3
- Repository ready for cutover to main branch
- Auto-generated README working
- Complete documentation and user guides

### Phase 4: Cutover & Launch (Week 8)
**Goal**: Deploy to production and launch v2.0

- [ ] Comprehensive testing of all components on `modernization` branch
- [ ] Performance tuning and optimization
- [ ] Security review of S3 configurations and API keys
- [ ] Final user documentation and tutorials
- [ ] Merge `modernization` branch to `main`
- [ ] Create GitHub release with full changelog
- [ ] Update repository description and tags
- [ ] Announce launch (social media, relevant communities)
- [ ] Monitor for issues and user feedback

## Migration Strategy

### Greenfield + Incremental Migration Approach

**Philosophy**: Build the ideal architecture from scratch, then migrate existing notebook logic incrementally.

**Why This Approach:**
- 76+ notebooks have inconsistent patterns and massive code duplication
- Extracting common patterns would result in compromise architecture
- Starting fresh allows optimal design for automation and maintainability
- Lower risk: each conversion is simple (extract scraping logic → implement `scrape()` method)

### Notebook to Python Class Conversion

**Before** (Jupyter Notebook):
```python
# Multiple cells with setup, scraping, processing, saving (~100+ lines)
%load_ext lab_black
import pandas as pd
import requests
import geopandas as gpd
# ... scraping code ...
# ... processing code ...
df.to_csv(f"data/processed/{place}_locations.csv", index=False)
```

**After** (Python Class):
```python
# src/scrapers/starbucks.py (~20-30 lines)
from src.core.base_scraper import BaseScraper
import requests

class StarbucksScraper(BaseScraper):
    def scrape(self) -> pd.DataFrame:
        # Only the core scraping logic
        # All setup, processing, saving handled by parent class
        return df
```

**Key Benefits:**
- ✅ Keep valuable scraping knowledge and logic
- ✅ Eliminate duplicated infrastructure code
- ✅ Each conversion is simple and testable
- ✅ Consistent patterns across all scrapers

### Conversion Script
```python
# scripts/convert_notebook.py
def convert_notebook_to_scraper(notebook_path: str, company_name: str):
    """Convert Jupyter notebook to Python scraper class"""
    # Parse notebook, extract scraping logic, generate Python class
    pass
```

### Data Migration Plan
1. **Backup**: Create full backup of existing data
2. **Upload**: Batch upload all existing files to S3 with timestamps
3. **Verify**: Ensure all data transferred correctly
4. **Update**: Modify any references to local files
5. **Cleanup**: Remove local data files (keep in .gitignore)

## Risk Mitigation

### Technical Risks
- **Geocoding Rate Limits**: Implement multiple geocoding services, caching
- **Website Changes**: Add monitoring for structural changes, fallback methods
- **S3 Costs**: Implement data lifecycle policies, compression
- **GitHub Actions Limits**: Use self-hosted runners if needed

### Operational Risks
- **Data Loss**: Comprehensive backups, versioning
- **Service Downtime**: Graceful degradation, retry logic
- **Team Adoption**: Extensive documentation, training materials

### Mitigation Strategies
- Gradual rollout with pilot companies
- Comprehensive testing at each phase
- Rollback plan to current system if needed
- Monitor data quality throughout migration

## Success Metrics

### Technical Metrics
- **Automation**: 100% of scrapers running automatically
- **Reliability**: >95% success rate for scheduled runs
- **Performance**: <2 hours for full scraping cycle
- **Data Quality**: >90% geocoding success rate

### Operational Metrics
- **Maintainability**: Time to add new company <30 minutes
- **Storage**: Repository size <100MB (vs current ~GB)
- **Documentation**: Auto-generated README always current
- **Error Resolution**: Average issue resolution time <24 hours

## Cost Analysis

### Current Costs
- **Developer Time**: High maintenance overhead
- **Storage**: Large repository size affects cloning/CI
- **Manual Updates**: Time spent on README maintenance

### New System Costs
- **S3 Storage**: ~$50-100/month for data storage
- **GitHub Actions**: Minimal (within free tier limits)
- **Development**: 8 weeks initial investment
- **Maintenance**: Significantly reduced ongoing time

### ROI Calculation
- **Time Saved**: ~80% reduction in maintenance time
- **Automation**: Eliminates manual update processes
- **Scalability**: Easy to add new companies
- **Data Access**: Better data discoverability and access

## Conclusion

This modernization plan transforms the locations repository from a collection of Jupyter notebooks into a scalable, automated data collection system. The investment in refactoring will pay dividends in reduced maintenance overhead, improved data quality, and easier expansion to new companies.

The phased approach ensures minimal disruption while steadily improving the system's capabilities. The new architecture will support the project's growth and make it easier to maintain the comprehensive dataset that has already proven valuable for research and journalism. 