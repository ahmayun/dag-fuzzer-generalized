#!/usr/bin/env python3
import json
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import re

def extract_timestamp(filename):
    """Extract timestamp from coverage filename."""
    match = re.search(r'coverage_(\d{8}_\d{6})\.cov', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d_%H%M%S')
    return None

def get_coverage_for_file(cov_file, target_file):
    """Get executed lines for a specific file from a coverage file."""
    try:
        # Generate JSON from .cov file
        result = subprocess.run(
            ['python', '-m', 'coverage', 'json', '--data-file', str(cov_file), '-o', '-'],
            capture_output=True,
            text=True,
            check=True
        )
        data = json.loads(result.stdout)
        
        # Normalize the target file path for comparison
        for filepath, coverage_info in data['files'].items():
            if Path(filepath).resolve() == Path(target_file).resolve() or filepath == target_file:
                return set(coverage_info.get('executed_lines', []))
        
        return set()
    except (subprocess.CalledProcessError, json.JSONDecodeError, KeyError):
        return set()

def find_first_covered(coverage_dir, target_file, line_number):
    """Find the two coverage files between which a line was first covered."""
    coverage_dir = Path(coverage_dir)
    
    # Get all .cov files sorted by timestamp
    cov_files = sorted(
        coverage_dir.glob('coverage_*.cov'),
        key=lambda f: extract_timestamp(f.name) or datetime.min
    )
    
    if not cov_files:
        print("No coverage files found", file=sys.stderr)
        return None
    
    print(f"Found {len(cov_files)} coverage files", file=sys.stderr)
    print(f"Searching for line {line_number} in {target_file}", file=sys.stderr)
    
    prev_file = None
    prev_covered = False
    
    for i, cov_file in enumerate(cov_files):
        print(f"Checking {cov_file.name} ({i+1}/{len(cov_files)})...", file=sys.stderr)
        
        executed_lines = get_coverage_for_file(cov_file, target_file)
        is_covered = line_number in executed_lines
        
        if is_covered and not prev_covered:
            # Found the transition!
            if prev_file:
                print(f"\nLine {line_number} was first covered between:", file=sys.stderr)
                print(prev_file.name)
                print(cov_file.name)
                return (prev_file, cov_file)
            else:
                print(f"\nLine {line_number} was already covered in the first file:", file=sys.stderr)
                print(cov_file.name)
                return (None, cov_file)
        
        prev_file = cov_file
        prev_covered = is_covered
    
    if not prev_covered:
        print(f"\nLine {line_number} was never covered in any file", file=sys.stderr)
    else:
        print(f"\nLine {line_number} was covered throughout all files", file=sys.stderr)
    
    return None

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python find-first-covered.py <coverage_dir> <file_path> <line_number>")
        print("Example: python find-first-covered.py ./consolidated-coverage /path/to/file.py 180")
        sys.exit(1)
    
    coverage_dir = sys.argv[1]
    target_file = sys.argv[2]
    
    try:
        line_number = int(sys.argv[3])
    except (IndexError, ValueError):
        print("Error: Line number must be an integer", file=sys.stderr)
        sys.exit(1)
    
    find_first_covered(coverage_dir, target_file, line_number)