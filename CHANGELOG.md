# Changelog

All notable changes to the locations project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0-alpha] - 2025-01-27

### 🎉 MAJOR MILESTONE: Locations v2.0 Modernization

This release represents a complete architectural overhaul from 76+ individual Jupyter notebooks to a scalable, automated scraping system.

### Added

#### Core Architecture
- **BaseScraper**: Abstract base class providing standardized scraping workflow
- **DataProcessor**: Centralized data cleaning, geocoding, and standardization
- **S3Storage**: Cloud storage with local fallback for all output formats
- **ScraperRegistry**: Auto-discovery and execution management for scrapers
- **Configuration System**: YAML-based configuration for companies and settings

#### Modern Development Environment
- **uv Package Management**: Fast, reliable dependency management with pyproject.toml
- **Makefile**: 20+ development commands for common tasks
- **Development Tools**: Black, Ruff, MyPy, Pytest integration
- **Setup Automation**: One-command project setup with `make setup`

#### CLI Interface
- **Batch Processing**: Run by company, category, or all scrapers
- **Parallel Execution**: Concurrent scraper execution with rate limiting
- **Dry Run Mode**: Test configurations without actual scraping
- **Comprehensive Logging**: Structured logging with configurable levels

#### Working Scrapers
- **In-N-Out Burger**: 423 locations via simple JSON API
- **Trader Joe's**: 623 locations via POST API with JSON payload
- **Starbucks**: Implementation complete (blocked by anti-bot protection)

#### Data Pipeline
- **Multi-format Output**: CSV, GeoJSON, JSON, and metadata files
- **S3 Storage**: Organized by company and timestamp (`s3://stilesdata.com/locations/`)
- **Data Quality Metrics**: Geocoding rates, validation, and completeness tracking
- **Error Handling**: Graceful failure handling with detailed error reporting

#### Testing & Quality
- **Test Suite**: Core component testing with pytest
- **Code Quality**: Automated linting, formatting, and type checking
- **Documentation**: Comprehensive development guides and API documentation

### Changed

#### From Jupyter Notebooks to Python Classes
- **Before**: 76+ individual notebooks with duplicated setup/processing code
- **After**: Modular Python classes inheriting from BaseScraper
- **Conversion Tool**: Automated notebook-to-Python conversion script

#### From Local Storage to Cloud
- **Before**: All data stored locally in repository
- **After**: S3 storage with organized structure and versioning
- **Fallback**: Local storage when S3 unavailable

#### From Manual to Automated
- **Before**: Manual execution of individual notebooks
- **After**: Automated batch processing with CLI interface
- **Scheduling**: Ready for GitHub Actions automation

### Technical Details

#### Dependencies Updated
- pandas: 1.5.0 → 2.3.1
- geopandas: 0.12.0 → 1.1.1  
- requests: 2.28.0 → 2.32.4
- Added: us, usaddress, geopy, boto3, pyogrio
- Dev tools: pytest, black, ruff, mypy

#### Performance Improvements
- **In-N-Out**: ~2.5 seconds for 423 locations
- **Trader Joe's**: ~4.9 seconds for 623 locations
- **Parallel Processing**: Multiple scrapers run concurrently
- **Rate Limiting**: Configurable delays to respect API limits

#### Configuration
```yaml
# config/companies.yaml - Company-specific settings
starbucks:
  name: "Starbucks"
  scraper_class: "StarbucksScraper"
  api_endpoint: "https://www.starbucks.com/bff/locations"
  rate_limit: 0.5

# config/settings.yaml - Global settings  
storage:
  s3_bucket: "stilesdata.com"
geocoding:
  service: "nominatim"
  rate_limit: 1.0
```

### Fixed

#### S3 Storage Configuration
- **Issue**: Storage class not reading bucket from configuration file
- **Fix**: Added fallback chain: parameter → environment variable → config file
- **Result**: S3 uploads working correctly to `s3://stilesdata.com/locations/`

#### JSON Serialization
- **Issue**: numpy int64 types not JSON serializable in metadata
- **Fix**: Explicit type conversion to Python native types
- **Result**: All metadata files generate correctly

#### GeoJSON S3 Upload
- **Issue**: GeoPandas to_file() incompatible with S3 StringIO
- **Fix**: Use BytesIO for binary GeoJSON data
- **Result**: GeoJSON files upload successfully to S3

### Development Workflow

#### New Commands
```bash
# Project setup
make setup                    # Complete project initialization
uv sync --dev                # Install all dependencies

# Development
make scrape COMPANY=in-n-out  # Run individual scraper
make test                     # Run test suite
make lint                     # Code quality checks
make format                   # Code formatting

# Batch operations
make scrape-all              # Run all scrapers
make scrape-category CATEGORY="Fast Food & Quick Service"
make dry-run                 # Test without execution
```

#### Files Added
- `pyproject.toml` - Modern Python project configuration
- `Makefile` - Development command automation
- `DEVELOPMENT.md` - Comprehensive developer guide
- `scripts/setup.py` - Automated project setup
- `scripts/convert_notebooks.py` - Notebook conversion utility
- `.python-version` - Python version pinning

### Metrics

#### Proof of Concept Results
- **Total Locations Scraped**: 1,046 locations
- **Scrapers Implemented**: 2/3 target companies
- **Success Rate**: 100% for working scrapers
- **Pipeline Tests**: Complete scrape → process → store → validate
- **Storage Verified**: S3 uploads confirmed working

#### Code Quality
- **Test Coverage**: Core components tested
- **Linting**: Ruff configuration with Python best practices
- **Type Checking**: MyPy integration for static analysis
- **Formatting**: Black code formatter with consistent style

### Documentation

#### Created
- `DEVELOPMENT.md` - Developer setup and workflow guide
- `CHANGELOG.md` - This comprehensive change log
- Updated `planning.md` - Reflect actual progress vs. plan

#### API Documentation
- All classes and methods documented with docstrings
- Configuration examples provided
- Error handling patterns documented

### Next Steps (Phase 2: Migration)

#### Week 3-5 Planning
1. **Week 3**: Convert 20-25 simpler scrapers using proven methodology
2. **Week 4**: Handle complex scrapers (pagination, auth, special cases)
3. **Week 5**: Complete GitHub Actions automation setup

#### Ready State
- ✅ Core architecture proven and tested
- ✅ Development environment optimized
- ✅ Conversion methodology established
- ✅ S3 storage pipeline working
- ✅ Documentation complete

### Breaking Changes

#### For Future Notebook Updates
- Notebooks in main branch will be moved to `notebooks-archive/` after full migration
- Data files will be stored in S3, not local repository
- New scrapers should be implemented as Python classes, not notebooks

#### Migration Path
- Existing notebooks remain functional during transition
- New architecture runs in parallel on `modernization` branch
- Cutover planned after all 76 scrapers converted

---

**Total Development Time**: ~8 hours over 2 sessions
**Architecture Quality**: Production-ready, scalable foundation
**Developer Experience**: Streamlined with modern tooling
**Deployment Ready**: S3 storage and automation-ready infrastructure 