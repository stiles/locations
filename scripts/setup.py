#!/usr/bin/env python3
"""
Setup script for the locations project using uv.
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle errors"""
    print(f"📦 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        if result.stdout:
            print(f"   {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: {e}")
        if e.stderr:
            print(f"   {e.stderr.strip()}")
        return False


def check_uv_installed():
    """Check if uv is installed"""
    try:
        subprocess.run(["uv", "--version"], check=True, capture_output=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def install_uv():
    """Install uv if not present"""
    print("🚀 Installing uv...")
    install_cmd = "curl -LsSf https://astral.sh/uv/install.sh | sh"
    return run_command(install_cmd, "Installing uv package manager")


def main():
    """Main setup function"""
    print("🗺️  Setting up Locations v2.0 project")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not Path("pyproject.toml").exists():
        print("❌ Error: pyproject.toml not found. Run this from the project root.")
        sys.exit(1)
    
    # Check/install uv
    if not check_uv_installed():
        print("📦 uv not found, installing...")
        if not install_uv():
            print("❌ Failed to install uv. Please install manually:")
            print("   curl -LsSf https://astral.sh/uv/install.sh | sh")
            sys.exit(1)
        
        # Add uv to PATH for current session
        home = os.path.expanduser("~")
        uv_bin = f"{home}/.cargo/bin"
        if uv_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{uv_bin}:{os.environ.get('PATH', '')}"
    else:
        print("✅ uv is already installed")
    
    # Create virtual environment and install dependencies
    success = True
    success &= run_command("uv sync --dev", "Creating virtual environment and installing dependencies")
    
    # Create necessary directories
    directories = ["logs", "data/processed", "data/raw", "data/cache"]
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    print(f"📁 Created directories: {', '.join(directories)}")
    
    # Run basic tests to verify setup
    if success:
        success &= run_command("uv run pytest tests/ -v", "Running basic tests to verify setup")
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 Setup complete!")
        print("\n📋 Next steps:")
        print("   • Run 'make help' to see available commands")
        print("   • Run 'make dry-run' to test scrapers")
        print("   • Run 'make scrape COMPANY=in-n-out' to test a working scraper")
        print("   • Run 'source .venv/bin/activate' to activate the virtual environment")
        print("\n🚀 Ready to start scraping!")
    else:
        print("❌ Setup encountered errors. Please check the output above.")
        sys.exit(1)


if __name__ == "__main__":
    main() 