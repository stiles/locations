#!/usr/bin/env python3
"""
Location scraper CLI interface

Usage:
    python scripts/run_scraper.py --company starbucks
    python scripts/run_scraper.py --category "Coffee Shops & Desserts"
    python scripts/run_scraper.py --all
"""

import sys
import os
import click
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.config import config
from src.utils.logging import setup_logging


@click.command()
@click.option('--company', help='Company name to scrape')
@click.option('--all', 'scrape_all', is_flag=True, help='Scrape all companies')
@click.option('--category', help='Scrape all companies in category')
@click.option('--config-dir', default='config', help='Config directory path')
@click.option('--dry-run', is_flag=True, help='Validate without running')
@click.option('--parallel', is_flag=True, help='Run multiple scrapers in parallel')
@click.option('--verbose', is_flag=True, help='Enable verbose logging')
def main(company, scrape_all, category, config_dir, dry_run, parallel, verbose):
    """Location scraper CLI"""
    
    # Set up logging
    log_level = "DEBUG" if verbose else config.get_setting("logging.level", "INFO")
    log_file = config.get_setting("logging.file", "logs/locations.log")
    setup_logging(log_level, log_file)
    
    click.echo("🗺️  Location Scraper v2.0")
    click.echo("=" * 40)
    
    # Load configuration
    config.config_dir = config_dir
    companies_config = config.load_companies()
    
    # Determine which companies to run
    if scrape_all:
        companies_to_run = config.get_all_company_names()
        click.echo(f"📊 Running ALL {len(companies_to_run)} companies")
    elif category:
        companies_to_run = config.get_companies_in_category(category)
        if not companies_to_run:
            click.echo(f"❌ No companies found in category: {category}")
            click.echo("Available categories:")
            categories = companies_config.get("categories", {})
            for cat_name in categories:
                click.echo(f"  - {cat_name}")
            return
        click.echo(f"📂 Running {len(companies_to_run)} companies in '{category}'")
    elif company:
        if company not in config.get_all_company_names():
            click.echo(f"❌ Company not found: {company}")
            click.echo("Available companies:")
            for comp_name in sorted(config.get_all_company_names()):
                click.echo(f"  - {comp_name}")
            return
        companies_to_run = [company]
        click.echo(f"🏢 Running single company: {company}")
    else:
        click.echo("❌ Specify --company, --category, or --all")
        return
    
    if dry_run:
        click.echo("\n🔍 DRY RUN - Would scrape:")
        for comp in companies_to_run:
            comp_config = config.get_company_config(comp)
            click.echo(f"  ✓ {comp} ({comp_config.get('name', 'Unknown')})")
        click.echo(f"\nTotal: {len(companies_to_run)} companies")
        return
    
    # Import scraper registry here to avoid import issues
    try:
        from src.scrapers.registry import ScraperRegistry
        registry = ScraperRegistry()
    except ImportError:
        click.echo("⚠️  Scraper registry not yet implemented")
        click.echo("📝 This will be available in Phase 2 of the implementation")
        click.echo("\n🎯 Current status: Core infrastructure complete")
        click.echo("   Next steps: Implement individual scrapers")
        return
    
    # Run scrapers
    click.echo(f"\n🚀 Starting scraping process...")
    click.echo(f"   Mode: {'Parallel' if parallel else 'Sequential'}")
    
    if parallel:
        results = registry.run_parallel(companies_to_run, companies_config)
    else:
        results = registry.run_sequential(companies_to_run, companies_config)
    
    # Print summary
    successful = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'error']
    
    click.echo(f"\n📈 SUMMARY")
    click.echo(f"=" * 40)
    click.echo(f"✅ Successful: {len(successful)}")
    click.echo(f"❌ Failed: {len(failed)}")
    
    if successful:
        click.echo(f"\n🎉 Successful companies:")
        for result in successful:
            locations_count = result.get('locations_count', 0)
            click.echo(f"  ✓ {result['company']}: {locations_count:,} locations")
    
    if failed:
        click.echo(f"\n💥 Failed companies:")
        for result in failed:
            click.echo(f"  ❌ {result['company']}: {result['error']}")


if __name__ == '__main__':
    main() 