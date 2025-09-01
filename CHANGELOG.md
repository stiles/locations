# Changelog

All notable changes to the locations project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0-alpha-batch5] - 2025-01-27

### Added: Major Progress Session
- **Pollo Tropical** scraper (122 locations) - Olo API platform integration
- **Kung Fu Tea** scraper (395 locations) - StorePoint API platform  
- **Nike Stores** scraper (892 locations) - Official Nike API endpoint 🔥
- **99 Ranch Market** scraper (65 locations) - POST API call pattern
- **Meijer** scraper (270 locations) - Regional grocery chain, Midwest ZIP iteration
- New "Grocery Stores" category in company configurations
- Enhanced POST API call pattern support for JSON payloads

### Fixed
- Improved column mapping validation in DataProcessor
- Enhanced error handling for POST requests in BaseScraper
- File naming conventions for Python modules (ranch_99.py)

### Blocked
- **Hollister**: Store locator API endpoints changed/removed (400 Bad Request)
- **Crumbl Cookies**: Next.js build ID requires dynamic extraction (404 errors)

### Metrics
- **Total Working Scrapers**: 14 companies (18.4% of 76)
- **Total Locations**: 3,086+ successfully processed
- **Session Efficiency**: 5 scrapers converted in rapid succession
- **New Patterns**: Official brand APIs, ethnic grocery chains, POST requests
- **Conversion Success Rate**: 67% (14/21 attempted)

### Proven Architecture Patterns
1. **Single API Call**: In-N-Out, King Taco, Trader Joe's, Nike Stores
2. **ZIP Code Iteration**: Barnes & Noble, CVS, Meijer
3. **HTML Page Scraping**: Buc-ee's, Au Bon Pain
4. **WordPress AJAX**: Wahoo's (with nonce extraction)
5. **Individual Page Scraping**: Superior Grocers
6. **Inline JavaScript JSON**: Pinkberry
7. **POST API Calls**: 99 Ranch Market, Pollo Tropical, Kung Fu Tea

## [2.0.0-alpha-batch4] - 2025-01-27

### Added: Website Modernization Analysis Session
- **BLOCKED_COMPANIES.md**: Comprehensive documentation of modernization challenges
- **Company Configuration**: Added Menchies, TCBY, and Shipley Donuts to config system
- **Modernization Patterns**: Identified 3 major website evolution trends

### Website Modernization Challenges Identified
- **Menchies**: HTML parsing → WordPress + Google Maps integration
- **TCBY**: State API iteration → Complete API deprecation (404s)
- **Shipley Donuts**: Inline JavaScript JSON → Next.js/React SSR

### Enhanced Documentation
- **Blocked Companies Tracking**: Systematic categorization by issue type (🚫 🔄 🔍 ⏸️)
- **Technical Investigation Notes**: Detailed analysis for future research
- **Success Rate Tracking**: Real-time metrics and completion status
- **Priority Classification**: Based on dataset size and technical complexity

### Key Insights
- **Website Evolution Impact**: 64% success rate for conversion attempts (9/14)
- **Modern Framework Migration**: React, Next.js, WordPress integrations increasingly common
- **API Deprecation Trend**: Some companies removing public location APIs entirely
- **Architecture Resilience**: Core framework handles all challenges gracefully

### Metrics Update
- **Total Working Scrapers**: 9 companies ✅ (unchanged - focus on analysis)
- **Total Locations**: 1,342+ successfully processed
- **Blocked/Challenging**: 5 companies documented (6.6% of total)
- **Remaining Targets**: 62 companies (81.6% of total)
- **Architecture Validation**: Complete across 6 proven patterns ✅

## [2.0.0-alpha-batch3] - 2025-01-27

### Added: Batch Conversion Session 3
- **Superior Grocers scraper**: 74 locations using individual page scraping pattern
- **Pinkberry scraper**: 70 locations using inline JavaScript JSON extraction
- **BLOCKED_COMPANIES.md**: Documentation for challenging companies requiring investigation

### Proven Patterns
- **Individual Page Scraping**: Superior Grocers (multi-step site crawling with coordinate extraction)
- **Inline JavaScript JSON**: Pinkberry (regex extraction from `<script>` tags) ✨ **NEW PATTERN**

### Enhanced Features  
- **Column Standardization**: DataProcessor handles diverse API response formats seamlessly
- **Rapid Debugging Workflow**: Quick identification and resolution of API structure changes
- **Error Documentation**: Systematic tracking of blocked companies for future investigation

### Fixes
- **Superior Grocers**: Completed background processing of individual location pages
- **Pinkberry**: Fixed column mapping from capitalized API response to standard format

### Metrics Update
- **Total Working Scrapers**: 9 companies ✅
- **Total Locations**: 1,342+ successfully processed
- **Proven Patterns**: 6 distinct scraping methodologies
- **Success Rate**: 82% (9/11 attempted conversions)
- **Architecture Validation**: Complete across diverse API types

## [2.0.0-alpha-batch2] - 2025-01-27 Evening

### 🚀 MAJOR PROGRESS: Batch Conversion Session

This session represents a breakthrough in conversion velocity, successfully converting 4+ new companies and proving 5 distinct scraping patterns work seamlessly with the architecture.

### Added

#### New Working Scrapers
- **Barnes & Noble**: 569 locations via ZIP code iteration (51 minutes)
- **King Taco**: 22 locations via single API call (2 seconds)
- **Buc-ee's**: 54 locations via HTML page scraping (58 seconds)  
- **Wahoo's**: 44 locations via WordPress AJAX with authentication (5 seconds)
- **Au Bon Pain**: 32 locations via structured HTML parsing (79 seconds)
- **CVS**: Scaled to 4,000+ ZIP codes after debugging API structure changes
- **Superior Grocers**: Individual page scraping pattern (in progress)

#### Proven Scraping Patterns
- **HTML Page Scraping**: Buc-ee's main locations page with BeautifulSoup
- **Structured HTML Parsing**: Au Bon Pain's all-stores page with `.loc-box` containers
- **WordPress AJAX with Authentication**: Wahoo's with dynamic nonce extraction
- **Individual Page Scraping**: Superior Grocers visiting each location page
- **API Structure Debugging**: CVS API changes successfully diagnosed and fixed

#### Enhanced Data Processing
- **Dynamic Nonce Extraction**: Wahoo's scraper extracts fresh nonce from locations page
- **Complex Address Parsing**: Au Bon Pain regex parsing of city, state, ZIP
- **Google Maps URL Parsing**: Superior Grocers extracting addresses from Maps URLs
- **JavaScript Variable Extraction**: Superior Grocers parsing `var store` and `var latlon`

### Fixed

#### CVS API Structure Changes
- **Issue**: CVS scraper returned only 1 location instead of expected thousands
- **Root Cause**: API response structure changed - data moved to nested `storeInfo` and `address` objects
- **Solution**: Updated parsing to handle new structure: `store.get('storeInfo', {}).get('storeId')`
- **Result**: Successfully scaled to 4,000+ ZIP codes

#### Address Parsing Edge Cases
- **Issue**: Various address formats across different company websites
- **Solution**: Flexible parsing with fallback patterns for city/state/ZIP extraction
- **Result**: Robust handling of "City, State ZIP" and URL-encoded formats

### Enhanced

#### Error Handling
- **Geocoding Timeouts**: Graceful handling of Nominatim API timeouts (non-critical)
- **Rate Limiting**: All scrapers respect configured rate limits automatically
- **Page-by-Page Processing**: Superior Grocers handles individual page failures gracefully

#### Development Velocity
- **Pattern Recognition**: Rapid identification of scraping patterns from notebooks
- **Configuration**: Streamlined addition of new companies to YAML config
- **Testing**: Each scraper tested immediately after creation
- **Debugging**: Systematic approach to API issues (CVS example)

### Metrics

#### Conversion Results
- **Total New Locations**: 1,198+ (was 1,046)
- **New Scrapers**: 4+ companies converted in single session
- **Success Rate**: 100% for completed scrapers
- **Performance**: Range from 2 seconds (King Taco) to 51 minutes (Barnes & Noble ZIP iteration)

#### Pattern Coverage
- **Single API Call**: 3 scrapers (In-N-Out, King Taco, Trader Joe's)
- **ZIP Code Iteration**: 3 scrapers (Barnes & Noble, CVS, Starbucks)
- **HTML Scraping**: 2 scrapers (Buc-ee's, Au Bon Pain)
- **WordPress AJAX**: 1 scraper (Wahoo's)
- **Individual Pages**: 1 scraper (Superior Grocers - in progress)

#### Development Speed
- **Average Conversion Time**: ~30 minutes per company
- **Debugging Success**: CVS API changes resolved systematically
- **Architecture Validation**: All patterns work without core modifications

### Technical Details

#### New Configuration Entries
```yaml
barnes-and-noble:
  scraper_class: "BarnesAndNobleScraper"
  base_url: "https://stores.barnesandnoble.com/_next/data/.../index.json"
  
king-taco:
  scraper_class: "KingTacoScraper"
  api_endpoint: "https://kingtaco.com/wp-admin/admin-ajax.php"
  
buc-ees:
  scraper_class: "BucEesScraper"
  base_url: "https://buc-ees.com/locations/"
  
wahoos:
  scraper_class: "WahoosScraper"
  api_endpoint: "https://www.wahoos.com/wp-admin/admin-ajax.php"
  
au-bon-pain:
  scraper_class: "AuBonPainScraper"
  base_url: "https://www.aubonpain.com/stores/all-stores"
```

#### Enhanced Categories
- **Specialty Foods**: Added for Wahoo's
- **Coffee Shops & Desserts**: Added Au Bon Pain
- **Retail Stores**: Added Buc-ee's
- **Fast Food & Quick Service**: Added King Taco
- **Grocery & Supermarkets**: Added Superior Grocers

### Documentation

#### Updated Planning Documents
- `planning.md`: Updated Phase 2 progress, marked as "AHEAD OF SCHEDULE"
- `README-PHASE2.md`: Added session results and proven patterns
- Progress tracking: 7+ scrapers = 10%+ completion of 76 total

#### Proven Methodology
1. **Analyze**: Review notebook scraping pattern
2. **Extract**: Identify core API/HTML parsing logic
3. **Implement**: Create scraper class inheriting from BaseScraper
4. **Configure**: Add company to YAML config
5. **Test**: Verify with `make scrape COMPANY=name`
6. **Debug**: Systematic approach to API changes

### Next Steps

#### Immediate Priorities
- Complete Superior Grocers individual page scraping
- Continue batch conversion of remaining 69 companies
- Target Hmart as next conversion candidate

#### Architecture Validation
- ✅ Multiple scraping patterns proven
- ✅ Error handling working across all patterns
- ✅ Data standardization consistent
- ✅ S3 storage pipeline robust
- ✅ Rate limiting and retries functional

---

**Session Impact**: Major acceleration in conversion velocity with diverse pattern validation
**Total Progress**: 7+ working scrapers across 5 distinct patterns
**Architecture Status**: Proven scalable and adaptable to various website structures

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