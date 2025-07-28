# Development Guide

## Quick Start with uv

This project uses [uv](https://github.com/astral-sh/uv) for fast, reliable Python package management.

### Initial Setup

```bash
# Option 1: Automated setup
python scripts/setup.py

# Option 2: Manual setup 
make setup

# Option 3: Step by step
curl -LsSf https://astral.sh/uv/install.sh | sh  # Install uv
uv sync --dev                                    # Install dependencies
```

### Daily Development

```bash
# See all available commands
make help

# Activate virtual environment  
source .venv/bin/activate  # OR use 'uv run <command>'

# Run tests
make test

# Run scrapers
make scrape COMPANY=in-n-out
make scrape-category CATEGORY="Fast Food & Quick Service"
make dry-run

# Code quality
make lint
make format
make type-check
```

### Package Management

```bash
# Add new dependency
uv add requests>=2.31.0

# Add development dependency  
uv add --dev pytest>=8.0.0

# Update all dependencies
make update-deps

# Show dependency tree
make show-deps
```

### Project Structure

```
├── src/                    # Main package code
│   ├── core/              # Core scraping infrastructure  
│   ├── scrapers/          # Individual company scrapers
│   └── utils/             # Shared utilities
├── scripts/               # CLI tools and utilities
├── tests/                 # Test suite
├── config/                # YAML configuration files
├── pyproject.toml         # Project metadata and dependencies
├── .python-version        # Python version (3.10)
└── Makefile              # Development commands
```

### Adding New Scrapers

1. Create scraper class in `src/scrapers/company_name.py`
2. Inherit from `BaseScraper` and implement `scrape()` method  
3. Add company config to `config/companies.yaml`
4. Test with `make scrape COMPANY=company-name`

### Testing

```bash
# Run all tests
make test

# Run with coverage
make test-cov

# Run specific test
uv run pytest tests/test_core.py -v
```

### Code Quality

This project uses:
- **Black**: Code formatting
- **Ruff**: Fast linting and import sorting  
- **MyPy**: Static type checking
- **Pytest**: Testing framework

```bash
# Check and fix all quality issues
make format lint type-check
```

## Architecture Overview

The v2.0 architecture provides:

- **Modular Design**: Each scraper is independent
- **Consistent Pipeline**: scrape → process → store → validate
- **Cloud Storage**: Automatic S3 upload with fallback to local
- **Error Handling**: Graceful failures don't break batch jobs
- **Data Quality**: Geocoding, validation, and metrics
- **Easy Scaling**: Add new companies in minutes

## Performance

- **In-N-Out**: 423 locations in ~2.5 seconds
- **Trader Joe's**: 623 locations in ~4.9 seconds  
- **Parallel Processing**: Multiple scrapers run concurrently
- **Rate Limiting**: Respects API limits and prevents blocking 