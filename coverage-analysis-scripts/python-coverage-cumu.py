#!/usr/bin/env python3
"""
Extract coverage metrics from Python coverage .cov files and generate a time series CSV.
"""

import argparse
import csv
import sys
from pathlib import Path
from datetime import datetime
import coverage


def parse_timestamp_from_filename(filename: str) -> datetime:
    """Extract timestamp from filename like 'coverage_20260127_115418.cov'"""
    # Extract the timestamp part: YYYYMMDD_HHMMSS
    parts = filename.replace('.cov', '').split('_')
    if len(parts) >= 3:
        date_str = parts[1]  # YYYYMMDD
        time_str = parts[2]  # HHMMSS
        timestamp_str = f"{date_str}_{time_str}"
        return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
    raise ValueError(f"Cannot parse timestamp from filename: {filename}")


def extract_coverage_metrics(cov_file: Path):
    """Extract coverage metrics from a .cov file"""
    cov = coverage.Coverage(data_file=str(cov_file), branch=True)
    cov.load()
    
    # Use coverage's built-in report to get correct totals
    from io import StringIO
    import sys

    # Capture the report output
    old_stdout = sys.stdout
    sys.stdout = report_output = StringIO()

    try:
        # Generate text report (this calculates everything correctly)
        total = cov.report(file=sys.stdout, show_missing=False)
    finally:
        sys.stdout = old_stdout

    # Get data object
    data = cov.get_data()
    has_arcs = data.has_arcs()
    num_files = len(data.measured_files())

    # Parse the report output to get statement metrics
    report_text = report_output.getvalue()
    lines = report_text.strip().split('\n')

    total_statements = 0
    total_missing = 0

    # Find the TOTAL line
    for line in lines:
        if line.startswith('TOTAL'):
            parts = line.split()
            if has_arcs:
                # Format: TOTAL  stmts  miss  branch  brpart  cover
                total_statements = int(parts[1])
                total_missing = int(parts[2])
            else:
                # Format: TOTAL  stmts  miss  cover
                total_statements = int(parts[1])
                total_missing = int(parts[2])

    total_covered = total_statements - total_missing

    # Calculate branch coverage using coverage's internal analysis
    total_branches = 0
    missing_branches = 0
    partial_branches = 0

    if has_arcs:
        for filename in data.measured_files():
            try:
                analysis = cov._analyze(filename)
                nums = analysis.numbers
                if hasattr(nums, 'n_branches'):
                    total_branches += nums.n_branches
                    missing_branches += nums.n_missing_branches
                    partial_branches += nums.n_partial_branches
            except:
                pass

    covered_branches = total_branches - missing_branches - partial_branches if total_branches > 0 else 0

    # Calculate percentages
    statements_pct = total if total is not None else 0.0
    branches_pct = (covered_branches * 100.0 / total_branches) if total_branches > 0 else 0.0

    metrics = {
        'num_files': num_files,
        'statements_covered': total_covered,
        'statements_total': total_statements,
        'statements_missing': total_missing,
        'statements_excluded': 0,
        'statements_pct': round(statements_pct, 2),
        'branches_covered': covered_branches,
        'branches_total': total_branches,
        'branches_missing': missing_branches,
        'branches_partial': partial_branches,
        'branches_pct': round(branches_pct, 2),
        'has_branch_coverage': has_arcs,
    }
    
    return metrics


def main():
    parser = argparse.ArgumentParser(
        description='Extract coverage metrics from .cov files and generate time series CSV'
    )
    parser.add_argument(
        '--input-dir',
        type=Path,
        required=True,
        help='Directory containing .cov files'
    )
    parser.add_argument(
        '--output-csv',
        type=Path,
        required=True,
        help='Output CSV file path'
    )
    parser.add_argument(
        '--points',
        type=int,
        default=None,
        help='Number of evenly spaced points to sample (processes all files if not specified)'
    )
    
    args = parser.parse_args()
    
    # Validate input directory
    if not args.input_dir.exists():
        print(f"Error: Input directory does not exist: {args.input_dir}", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory if needed
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    
    # Find all .cov files and sort by timestamp
    cov_files = sorted(args.input_dir.glob("coverage_*.cov"))
    
    if not cov_files:
        print(f"Error: No .cov files found in {args.input_dir}", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(cov_files)} coverage files")
    
    # Sample evenly spaced points if --points is specified
    if args.points is not None:
        if args.points < 2:
            print(f"Error: --points must be at least 2, got {args.points}", file=sys.stderr)
            sys.exit(1)
        
        total_files = len(cov_files)
        if args.points >= total_files:
            print(f"Note: Requested {args.points} points but only {total_files} files available, using all files")
        else:
            # Always include first and last, sample evenly in between
            if args.points == 2:
                # Just first and last
                sampled = [cov_files[0], cov_files[-1]]
            else:
                # First, last, and evenly spaced points in between
                sampled = [cov_files[0]]
                # Calculate indices for middle points (excluding first and last)
                step = (total_files - 1) / (args.points - 1)
                for i in range(1, args.points - 1):
                    idx = int(round(i * step))
                    sampled.append(cov_files[idx])
                sampled.append(cov_files[-1])
            
            cov_files = sampled
            print(f"Sampling {len(cov_files)} evenly spaced points from {total_files} total files (first and last always included)")
    
    # Process each coverage file
    rows = []
    start_time = None
    
    for cov_file in cov_files:
        try:
            # Parse timestamp from filename
            timestamp = parse_timestamp_from_filename(cov_file.name)
            
            if start_time is None:
                start_time = timestamp
            
            # Calculate elapsed time
            elapsed = timestamp - start_time
            seconds_elapsed = elapsed.total_seconds()
            hours_elapsed = seconds_elapsed / 3600.0
            
            # Extract coverage metrics
            metrics = extract_coverage_metrics(cov_file)
            
            # Build row
            row = {
                'timestamp': timestamp.isoformat(),
                'epoch_seconds': int(timestamp.timestamp()),
                'seconds_elapsed': int(seconds_elapsed),
                'hours_elapsed': round(hours_elapsed, 4),
                'num_files': metrics['num_files'],
                'statements_pct': metrics['statements_pct'],
                'statements_covered': metrics['statements_covered'],
                'statements_total': metrics['statements_total'],
                'statements_missing': metrics['statements_missing'],
                'statements_excluded': metrics['statements_excluded'],
                'branches_pct': metrics['branches_pct'],
                'branches_covered': metrics['branches_covered'],
                'branches_total': metrics['branches_total'],
                'branches_missing': metrics['branches_missing'],
                'branches_partial': metrics['branches_partial'],
                'has_branch_coverage': metrics['has_branch_coverage'],
                'cov_file': cov_file.name,
            }
            
            rows.append(row)
            
            # Print progress with branch info if available
            if metrics['has_branch_coverage']:
                print(f"Processed: {cov_file.name} - {metrics['statements_pct']:.2f}% stmts, {metrics['branches_pct']:.2f}% branches")
            else:
                print(f"Processed: {cov_file.name} - {metrics['statements_pct']:.2f}% statements")
            
        except Exception as e:
            print(f"Warning: Failed to process {cov_file.name}: {e}", file=sys.stderr)
            continue
    
    if not rows:
        print("Error: No valid coverage data extracted", file=sys.stderr)
        sys.exit(1)
    
    # Write CSV
    fieldnames = [
        'timestamp',
        'epoch_seconds',
        'seconds_elapsed',
        'hours_elapsed',
        'num_files',
        'statements_pct',
        'statements_covered',
        'statements_total',
        'statements_missing',
        'statements_excluded',
        'branches_pct',
        'branches_covered',
        'branches_total',
        'branches_missing',
        'branches_partial',
        'has_branch_coverage',
        'cov_file',
    ]
    
    with open(args.output_csv, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"\nSuccessfully wrote {len(rows)} rows to {args.output_csv}")
    print(f"Statement coverage range: {rows[0]['statements_pct']:.2f}% → {rows[-1]['statements_pct']:.2f}%")
    if rows[0]['has_branch_coverage']:
        print(f"Branch coverage range: {rows[0]['branches_pct']:.2f}% → {rows[-1]['branches_pct']:.2f}%")


if __name__ == '__main__':
    main()
