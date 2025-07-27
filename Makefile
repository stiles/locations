.PHONY: help install install-dev sync test lint format clean run-tests setup

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies with uv
	uv sync

install-dev: ## Install development dependencies
	uv sync --dev

sync: ## Sync dependencies (like pip-sync)
	uv sync --dev

test: ## Run tests
	uv run pytest

test-cov: ## Run tests with coverage
	uv run pytest --cov=src --cov-report=html --cov-report=term

lint: ## Run linting (ruff)
	uv run ruff check src/ tests/ scripts/

lint-fix: ## Fix linting issues
	uv run ruff check --fix src/ tests/ scripts/

format: ## Format code with black and ruff
	uv run black src/ tests/ scripts/
	uv run ruff format src/ tests/ scripts/

format-check: ## Check if code is formatted
	uv run black --check src/ tests/ scripts/
	uv run ruff format --check src/ tests/ scripts/

type-check: ## Run type checking with mypy
	uv run mypy src/

clean: ## Clean up build artifacts and caches
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete

# Scraper commands
scrape: ## Run scraper (usage: make scrape COMPANY=starbucks)
	uv run python scripts/run_scraper.py --company $(COMPANY)

scrape-all: ## Run all scrapers
	uv run python scripts/run_scraper.py --all

scrape-category: ## Run scrapers by category (usage: make scrape-category CATEGORY="Fast Food")
	uv run python scripts/run_scraper.py --category "$(CATEGORY)"

dry-run: ## Dry run all scrapers
	uv run python scripts/run_scraper.py --dry-run --all

# Development setup
setup: ## Initial project setup with uv
	@echo "Setting up project with uv..."
	@if ! command -v uv &> /dev/null; then \
		echo "Installing uv..."; \
		curl -LsSf https://astral.sh/uv/install.sh | sh; \
	fi
	uv sync --dev
	@echo "✅ Setup complete! Run 'make help' for available commands."

venv: ## Create virtual environment with uv
	uv venv

shell: ## Activate virtual environment
	uv shell

# Utility commands
convert-notebooks: ## Convert notebooks to Python files for analysis
	uv run python scripts/convert_notebooks.py

update-deps: ## Update dependencies
	uv lock --upgrade

show-deps: ## Show dependency tree
	uv tree 