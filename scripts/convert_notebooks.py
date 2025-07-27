#!/usr/bin/env python3
"""
Convert Jupyter notebooks to Python files for easier analysis
"""

import json
import sys
from pathlib import Path


def convert_notebook_to_py(notebook_path: Path, output_path: Path):
    """Convert a Jupyter notebook to a Python file"""
    
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook = json.load(f)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(f"# Converted from {notebook_path.name}\n")
        f.write(f"# Original notebook: {notebook_path}\n\n")
        
        for i, cell in enumerate(notebook.get('cells', [])):
            cell_type = cell.get('cell_type', 'unknown')
            
            if cell_type == 'code':
                source = cell.get('source', [])
                if source:
                    f.write(f"# ===== CELL {i} (CODE) =====\n")
                    if isinstance(source, list):
                        f.write(''.join(source))
                    else:
                        f.write(source)
                    f.write("\n\n")
                    
            elif cell_type == 'markdown':
                source = cell.get('source', [])
                if source:
                    f.write(f"# ===== CELL {i} (MARKDOWN) =====\n")
                    lines = source if isinstance(source, list) else [source]
                    for line in lines:
                        f.write(f"# {line}")
                    f.write("\n\n")


def main():
    """Convert target company notebooks to Python files"""
    
    companies = ['starbucks', 'trader-joes']
    
    # Create output directory
    output_dir = Path('notebooks_as_py')
    output_dir.mkdir(exist_ok=True)
    
    for company in companies:
        company_dir = Path(company)
        if not company_dir.exists():
            print(f"⚠️  Directory not found: {company}")
            continue
            
        # Find notebook files
        notebook_files = list(company_dir.glob('*.ipynb'))
        
        if not notebook_files:
            print(f"⚠️  No notebooks found in {company}")
            continue
            
        for notebook_path in notebook_files:
            output_path = output_dir / f"{company}_{notebook_path.stem}.py"
            
            try:
                convert_notebook_to_py(notebook_path, output_path)
                print(f"✅ Converted: {notebook_path} -> {output_path}")
            except Exception as e:
                print(f"❌ Error converting {notebook_path}: {e}")


if __name__ == '__main__':
    main() 