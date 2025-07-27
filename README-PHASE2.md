# Phase 2 Quick Start Guide

## 🎯 Current Status (2025-01-27)

**Phase 1 COMPLETE** ✅ - Core architecture built and tested with 1,046 locations scraped successfully.

**Ready for Phase 2**: Convert remaining 74 company scrapers from Jupyter notebooks to the new Python class architecture.

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
```

### 3. Development Commands

```bash
make help                       # See all available commands
make scrape COMPANY=<name>      # Run specific scraper
make test                       # Run test suite
make lint                       # Check code quality
```

## 📋 Phase 2 Tasks (Weeks 3-5)

### Week 3: Batch Conversion Strategy 🎯 **NEXT**

**Goal**: Convert 20-25 simpler scrapers

**Target Companies** (simple API patterns):
- costco, cvs, dunkin-donuts, krispy-kreme, home-depot
- apple-stores, autozone, bass-pro-shops, dsw, forever-21
- giant, hardees, hmart, hollister, hyundai
- jared, kfc, kroger, nordstrom, olive-garden

**Process**:
1. Convert notebook to Python: `make convert-notebooks`
2. Extract API logic from generated `.py` files
3. Create new scraper class in `src/scrapers/company_name.py`
4. Add configuration to `config/companies.yaml`
5. Test: `make scrape COMPANY=company-name`

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
- **Total**: 1,046 locations successfully stored to S3
- **Pipeline**: Complete scrape → process → store → validate workflow

### Phase 2 Goals 🎯
- Convert 76 total company scrapers
- Maintain or improve scraping success rates
- Automated batch processing working
- GitHub Actions deployment ready

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
uv shell                        # Activate environment
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