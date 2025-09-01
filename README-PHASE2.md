# Locations v2.0 - Phase 2 Quick Start

## Current Status (2025-01-27)

**PHASE 2 IN PROGRESS - MIXED RESULTS** ⚠️

- ✅ **15 scrapers converted** (target: 20-25 for Week 3)
- ⚠️ **~3,000+ locations scraped** - **BUT several incomplete implementations**
- ✅ **7 distinct scraping patterns proven**
- ✅ **Architecture fully validated** across diverse APIs
- 🚨 **CRITICAL**: Some "working" scrapers have significant under-coverage vs. original notebooks

### Conversion Status

**✅ Complete/High-Coverage Scrapers** (verified ~95%+ coverage):
1. **In-N-Out**: 423 locations (Single API) ✅
2. **Trader Joe's**: 623 locations (Single API) ✅
3. **King Taco**: 22 locations (Single API) ✅
4. **Buc-ee's**: 54 locations (HTML scraping) ✅
5. **Wahoo's**: 44 locations (WordPress AJAX) ✅
6. **Au Bon Pain**: 32 locations (HTML scraping) ✅
7. **Superior Grocers**: 74 locations (Individual pages) ✅
8. **Pinkberry**: 70 locations (Inline JavaScript JSON) ✅
9. **Pollo Tropical**: 122 locations (POST API call) ✅
10. **Kung Fu Tea**: 395 locations (StorePoint API) ✅
11. **Nike Stores**: 892 locations (Official Nike API) 🔥
12. **99 Ranch Market**: 65 locations (POST API call) ✅
13. **Meijer**: 270 locations (ZIP iteration) ✅
14. **Nordstrom**: 380 locations (ZIP iteration) ✅

**⚠️ Incomplete/Needs Original Logic** (significant under-coverage):
- **Barnes & Noble**: 569 locations vs. ~630 expected (need full ZIP approach)
- **Wendy's**: 386 locations vs. ~7,000 expected (strategic ZIP approach failed)
- **Cinemark**: 51 locations vs. 305+ expected (rate limited, need better approach)

**Blocked for Investigation**:
- **Starbucks**: Anti-bot protection (code complete)
- **Hmart**: Website migration to React platform  
- **Menchies**: WordPress + Google Maps modernization
- **TCBY**: Complete API deprecation (404 errors)
- **Shipley Donuts**: Next.js/React SSR migration
- **Hollister**: Store locator API endpoints changed
- **Crumbl Cookies**: Next.js build ID changes with deployments

### Proven Scraping Patterns

1. ✅ **Single API Call** - Clean REST/JSON endpoints
2. ✅ **ZIP Code Iteration** - Location-based API queries  
3. ✅ **HTML Page Scraping** - BeautifulSoup parsing
4. ✅ **WordPress AJAX with Auth** - Dynamic nonce extraction
5. ✅ **Individual Page Scraping** - Multi-step site crawling
6. ✅ **Inline JavaScript JSON** - Script tag data extraction
7. ✅ **POST API Calls** - JSON payload requests ✨ **NEW!**

## 🚧 Latest Session: Data Quality & Conversion Analysis

### Key Findings
- **Complete Conversion Rate**: 58% (14 complete / 24 attempted)
- **Incomplete Conversions**: 3 scrapers with significant under-coverage
- **Major Challenge**: Maintaining original notebook coverage standards
- **Critical Lesson**: Must validate against original location counts
- **Framework Requirement**: Stick to original logic where websites unchanged

### Modernization Patterns Identified
1. **WordPress Integration**: Static HTML → WordPress + Google Maps (Menchies)
2. **API Deprecation**: State APIs → Complete removal (TCBY) 
3. **React/Next.js Migration**: Embedded JSON → Server-side rendering (Shipley Donuts)
4. **Anti-bot Protection**: Standard APIs → Cloudflare blocking (Starbucks)
5. **SPA Migration**: Static pages → React SPAs (Hmart)

### Strategic Implications
- **Maintain Coverage Standards**: 95%+ location coverage required for "success"
- **Respect Original Logic**: If website unchanged, stick to proven notebook approach
- **Validate Against Known Counts**: Compare results to public location information
- **Complete vs. Fast**: Prefer comprehensive coverage over quick partial results
- **Investigation Pipeline**: 3 incomplete + 7 blocked companies need work

## 🚀 Resume Development

### 1. Environment Setup

```bash
# Switch to development branch
git checkout modernization

# Install dependencies (one-time setup)
make setup

# Or manually:
uv sync --dev
```

### 2. Verify Working State

```bash
# Test current infrastructure
make dry-run                    # Shows all configured companies
make scrape COMPANY=in-n-out    # Test working scraper (423 locations)
make scrape COMPANY=trader-joes # Test working scraper (623 locations)
make scrape COMPANY=pinkberry   # Test latest working scraper (70 locations)
```

### 3. Development Commands

```bash
make help                       # See all available commands
make scrape COMPANY=<name>      # Run specific scraper
make test                       # Run test suite
make lint                       # Check code quality
```

## 📋 Phase 2 Tasks (Weeks 3-5)

### Conversion Standards ⚠️ **CRITICAL REQUIREMENTS**

**Coverage Requirement**: All conversions must achieve **95%+ location coverage** matching original notebooks.

**Methodology**: 
1. **If website architecture unchanged**: Use original notebook logic exactly
2. **If API endpoints same**: Preserve original approach (ZIP iterations, pagination, etc.)
3. **If only minor changes**: Debug and adapt original logic rather than shortcuts
4. **Only if major modernization**: Consider alternative approaches

### Week 3: Batch Conversion Strategy ✅ **MIXED RESULTS**

**Goal**: Convert 20-25 simpler scrapers 
**Results**: 14 complete + 3 incomplete conversions

**✅ COMPLETED This Session**:
- **barnes-and-noble**: 569 locations (ZIP iteration) - 51 minutes
- **cvs**: 4000+ ZIPs (ZIP iteration, API debug & fix)
- **king-taco**: 22 locations (Single API call) - 2 seconds  
- **buc-ees**: 54 locations (HTML scraping) - 58 seconds
- **wahoos**: 44 locations (WordPress AJAX + auth) - 5 seconds
- **au-bon-pain**: 32 locations (HTML page parsing) - 79 seconds
- **superior-grocers**: In progress (Individual page scraping)

**🚀 PROVEN SCRAPING PATTERNS**:
1. **Single API Call** (In-N-Out, King Taco, Trader Joe's)
2. **ZIP Code Iteration** (Barnes & Noble, CVS, Starbucks)  
3. **HTML Page Scraping** (Buc-ee's, Au Bon Pain)
4. **WordPress AJAX with Auth** (Wahoo's with dynamic nonce)
5. **Individual Page Scraping** (Superior Grocers)
6. **Data Standardization** working across ALL patterns

**Proven Process** ✅:
1. ✅ Analyze notebook scraping pattern
2. ✅ Create scraper class in `src/scrapers/company_name.py`
3. ✅ Add configuration to `config/companies.yaml`
4. ✅ Test: `make scrape COMPANY=company-name`
5. ✅ Debug API changes (CVS example)

### Week 4: Complex Scrapers
**Goal**: Handle pagination, auth, special cases

### Week 5: Automation Setup  
**Goal**: GitHub Actions, scheduling, monitoring

## 🏗️ Architecture Overview

### Working Examples

**Simple JSON API** (In-N-Out pattern):
```python
class CompanyScraper(BaseScraper):
    def scrape(self) -> pd.DataFrame:
        response = self.session.get(api_url)
        data = response.json()
        return pd.DataFrame(data)
```

**POST API with JSON** (Trader Joe's pattern):
```python
class CompanyScraper(BaseScraper): 
    def scrape(self) -> pd.DataFrame:
        response = self.session.post(api_url, json=payload)
        data = response.json()
        return pd.DataFrame(data['results'])
```

**ZIP Code Iteration** (Starbucks pattern):
```python
class CompanyScraper(BaseScraper):
    def scrape(self) -> pd.DataFrame:
        # Load ZIP codes from _reference/data/zips_reference.csv
        # Iterate through ZIPs with rate limiting
        # Aggregate and deduplicate results
        return df
```

### Configuration

Add to `config/companies.yaml`:
```yaml
company-name:
  name: "Company Name"
  category: "Category"
  scraper_class: "CompanyNameScraper"
  api_endpoint: "https://api.example.com/locations"
  rate_limit: 1.0
  timeout: 30
```

## 📊 Success Metrics

### Phase 1 Results ✅
- **In-N-Out**: 423 locations in ~2.5 seconds
- **Trader Joe's**: 623 locations in ~4.9 seconds  
- **Original Total**: 1,046 locations successfully stored to S3
- **Pipeline**: Complete scrape → process → store → validate workflow

### Phase 2 Progress ⚠️ **MIXED RESULTS - QUALITY OVER SPEED**
- **Complete Conversions**: 14 companies with 95%+ coverage 
- **Incomplete Conversions**: 3 companies needing original notebook logic
- **Total Locations**: ~3,000+ locations, but some implementations incomplete
- **Key Learning**: Must prioritize coverage accuracy over conversion speed
- **Next Priority**: Fix incomplete implementations before adding new companies

### Updated Goals 🎯
- Convert 76 total company scrapers (7+ complete = 10%+ done)
- ✅ Proven architecture handles diverse patterns
- ✅ Maintain/improve scraping success rates
- Ready for GitHub Actions deployment

## 🔧 Development Tools

### Code Quality
```bash
make format                     # Auto-format code
make lint                       # Check linting
make type-check                 # Static type analysis
```

### Testing
```bash
make test                       # Run all tests
make test-cov                   # With coverage report
uv run pytest tests/test_core.py -v  # Specific tests
```

### Notebook Analysis
```bash
make convert-notebooks          # Convert .ipynb to .py for analysis
# Check notebooks_as_py/ directory for converted files
```

## 📚 Documentation

- `DEVELOPMENT.md` - Complete development guide
- `CHANGELOG.md` - Detailed progress tracking
- `planning.md` - Original plan with current status
- `pyproject.toml` - Modern Python project configuration

## 🆘 Troubleshooting

### Common Issues

**S3 Storage**: Ensure AWS profile 'haekeo' is configured
```bash
aws configure list-profiles
```

**Virtual Environment**: 
```bash
source .venv/bin/activate        # Activate environment
uv sync --dev                   # Reinstall if needed
```

**Import Errors**:
```bash
# Add project root to Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Getting Help

1. Check `make help` for available commands
2. Review `DEVELOPMENT.md` for detailed setup
3. Check `CHANGELOG.md` for recent changes
4. Test with working scrapers first: `make scrape COMPANY=in-n-out`

---

**Ready to continue the modernization! 🚀** 