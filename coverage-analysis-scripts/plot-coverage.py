#!/usr/bin/env python3

import argparse
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
import seaborn as sns
from matplotlib.ticker import LogLocator, LogFormatter

# Set better matplotlib style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")
EXPERIMENT_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
DIR_EXPERIMENT_ROOT = Path(f"coverage-plotting-runs/exp-{EXPERIMENT_TIMESTAMP}")


def print_and_log(*args, **kwargs):
    log_file = DIR_EXPERIMENT_ROOT / "log.txt"

    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "\n")

    text = sep.join(str(arg) for arg in args) + end

    with open(log_file, "a") as f:
        f.write(text)

    print(*args, **kwargs)


class CoveragePlotter:
    def __init__(self, dump_dir="coverage-dumps", jacoco_path="jacoco-0.8.13/lib/jacococli.jar", libs="lib/", coverage_pattern=None):
        self.dump_dir = Path(dump_dir)
        self.jacoco_path = jacoco_path
        self.libs = libs
        self.reports_dir = DIR_EXPERIMENT_ROOT / "reports"
        self.coverage_pattern = re.compile(coverage_pattern) if coverage_pattern else None

        # Create reports directory
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def _extract_timestamp_from_filename(self, filename):
        """Extract timestamp from filename like 'coverage-dump_20241124_143022.exec'"""
        match = re.search(r'coverage-dump_(\d{8}_\d{6})\.exec', filename)
        if match:
            timestamp_str = match.group(1)
            return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
        return None

    def _run_jacoco_report(self, dump_file, csv_file):
        """Run JaCoCo CLI to generate CSV report from dump file"""
        cmd = [
            'java', '-jar', self.jacoco_path, 'report', str(dump_file),
            '--classfiles', self.libs,
            '--csv', str(csv_file)
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print_and_log(f"✓ Generated report for {dump_file.name}")
            return True
        except subprocess.CalledProcessError as e:
            print_and_log(f"✗ Failed to generate report for {dump_file.name}: {e}")
            return False

    def _filter_coverage_data(self, df):
        """Filter coverage data based on the regex pattern"""
        if self.coverage_pattern is None:
            return df

        # Filter rows where any of the relevant columns match the pattern
        # Typical JaCoCo CSV columns include GROUP, PACKAGE, CLASS
        filter_columns = ['GROUP', 'PACKAGE', 'CLASS']

        # Check which columns exist in the dataframe
        existing_columns = [col for col in filter_columns if col in df.columns]

        if not existing_columns:
            print_and_log(f"⚠️  Warning: No filterable columns found ({filter_columns}). Using all data.")
            return df

        # Create a mask for rows that match the pattern in any of the existing columns
        mask = pd.Series([False] * len(df))

        for col in existing_columns:
            # Convert to string and apply regex matching
            col_mask = df[col].astype(str).str.match(self.coverage_pattern, na=False)
            mask = mask | col_mask

        filtered_df = df[mask]

        if len(filtered_df) == 0:
            print_and_log(f"⚠️  Warning: No data matched pattern '{self.coverage_pattern.pattern}'. Using all data.")
            return df

        print_and_log(f"📊 Filtered from {len(df)} to {len(filtered_df)} rows matching pattern '{self.coverage_pattern.pattern}'")
        return filtered_df

    def process_dumps(self):
        """Process all coverage dump files and create time-series data"""

        # Find all coverage dump files
        dump_files = list(self.dump_dir.glob('coverage-dump_*.exec'))

        if not dump_files:
            print_and_log(f"❌ No coverage dump files found in {self.dump_dir}")
            print_and_log(f"   Looking for files matching pattern: coverage-dump_*.exec")
            return pd.DataFrame()

        print_and_log(f"📁 Found {len(dump_files)} coverage dump files")
        if self.coverage_pattern:
            print_and_log(f"🔍 Will filter coverage data using pattern: '{self.coverage_pattern.pattern}'")

        # Sort by timestamp
        dump_files.sort(key=lambda x: self._extract_timestamp_from_filename(x.name) or datetime.min)

        # Process each dump file
        coverage_data = []

        for i, dump_file in enumerate(dump_files):
            timestamp = self._extract_timestamp_from_filename(dump_file.name)
            if not timestamp:
                print_and_log(f"⚠️  Skipping {dump_file.name} - couldn't parse timestamp")
                continue

            csv_file = self.reports_dir / f"report_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"

            print_and_log(f"🔄 Processing {dump_file.name} ({i+1}/{len(dump_files)})...")

            if self._run_jacoco_report(dump_file, csv_file):
                try:
                    # Read CSV and apply filtering if pattern is specified
                    df = pd.read_csv(csv_file)

                    # Apply regex filtering if pattern is specified
                    df_filtered = self._filter_coverage_data(df)

                    # Calculate totals from filtered data
                    total_inst_covered = df_filtered['INSTRUCTION_COVERED'].sum()
                    total_inst_missed = df_filtered['INSTRUCTION_MISSED'].sum()
                    total_lines_covered = df_filtered['LINE_COVERED'].sum()
                    total_lines_missed = df_filtered['LINE_MISSED'].sum()
                    total_methods_covered = df_filtered['METHOD_COVERED'].sum()
                    total_methods_missed = df_filtered['METHOD_MISSED'].sum()
                    total_branches_covered = df_filtered['BRANCH_COVERED'].sum()
                    total_branches_missed = df_filtered['BRANCH_MISSED'].sum()

                    # Calculate totals and percentages
                    total_inst = total_inst_covered + total_inst_missed
                    total_lines = total_lines_covered + total_lines_missed
                    total_methods = total_methods_covered + total_methods_missed
                    total_branches = total_branches_covered + total_branches_missed

                    inst_pct = (total_inst_covered / total_inst * 100) if total_inst > 0 else 0
                    line_pct = (total_lines_covered / total_lines * 100) if total_lines > 0 else 0
                    method_pct = (total_methods_covered / total_methods * 100) if total_methods > 0 else 0
                    branch_pct = (total_branches_covered / total_branches * 100) if total_branches > 0 else 0

                    coverage_data.append({
                        'timestamp': timestamp,
                        'instruction_coverage_percent': inst_pct,
                        'line_coverage_percent': line_pct,
                        'method_coverage_percent': method_pct,
                        'branch_coverage_percent': branch_pct,
                        'total_instructions_covered': total_inst_covered,
                        'total_lines_covered': total_lines_covered,
                        'total_methods_covered': total_methods_covered,
                        'total_branches_covered': total_branches_covered,
                        'total_instructions': total_inst,
                        'total_lines': total_lines,
                        'total_methods': total_methods,
                        'total_branches': total_branches
                    })

                except Exception as e:
                    print_and_log(f"✗ Error processing {csv_file}: {e}")

        if not coverage_data:
            print_and_log("❌ No valid coverage data found!")
            return pd.DataFrame()

        df_coverage = pd.DataFrame(coverage_data)
        df_coverage = df_coverage.sort_values('timestamp')

        # Calculate time elapsed from start
        start_time = df_coverage['timestamp'].iloc[0]
        df_coverage['time_elapsed_minutes'] = (df_coverage['timestamp'] - start_time).dt.total_seconds() / 60
        df_coverage['time_elapsed_hours'] = df_coverage['time_elapsed_minutes'] / 60

        return df_coverage

    def create_plots(self, df, show_plots=True, save_plots=True):
        """Create comprehensive coverage vs time plots"""

        print("PLOTTING DF:")
        print(df.head())
        if df.empty:
            print_and_log("❌ No data to plot!")
            return

        # Determine time unit based on data range
        total_minutes = df['time_elapsed_minutes'].max()
        if total_minutes < 120:  # Less than 2 hours
            time_col = 'time_elapsed_minutes'
            time_label = 'Time Elapsed (minutes)'
        else:
            time_col = 'time_elapsed_hours'
            time_label = 'Time Elapsed (hours)'

        # Use linear scale for small datasets or when max time is small
        use_log_scale = len(df) > 5 and total_minutes > 10

        # Handle the time=0 issue - always add small offset to avoid log(0) issues
        df_plot = df.copy()
        if time_col == 'time_elapsed_minutes':
            time_offset = 0.1  # 0.1 minutes = 6 seconds
        else:
            time_offset = 0.01  # 0.01 hours = 36 seconds

        # Add offset to handle time=0, regardless of scale type
        df_plot[time_col] = df_plot[time_col] + time_offset

        # Create figure with subplots
        fig = plt.figure(figsize=(16, 12))

        # Add pattern info to title if filtering is applied
        title_suffix = ""
        if self.coverage_pattern:
            title_suffix = f" (Filtered: {self.coverage_pattern.pattern})"

        # Main coverage percentages plot (larger)
        ax1 = plt.subplot2grid((3, 2), (0, 0), colspan=2)
        if use_log_scale:
            ax1.set_xscale('log')

        # Use larger markers and thicker lines for better visibility with few data points
        marker_size = 8 if len(df) <= 5 else 6
        line_width = 3 if len(df) <= 5 else 2.5

        ax1.plot(df_plot[time_col], df['instruction_coverage_percent'], 'o-',
                label='Instruction Coverage', linewidth=line_width, markersize=marker_size)
        ax1.plot(df_plot[time_col], df['line_coverage_percent'], 's-',
                label='Line Coverage', linewidth=line_width, markersize=marker_size)
        ax1.plot(df_plot[time_col], df['method_coverage_percent'], '^-',
                label='Method Coverage', linewidth=line_width, markersize=marker_size)
        ax1.plot(df_plot[time_col], df['branch_coverage_percent'], 'd-',
                label='Branch Coverage', linewidth=line_width, markersize=marker_size)

        ax1.set_xlabel(time_label, fontsize=12)
        ax1.set_ylabel('Coverage Percentage (%)', fontsize=12)
        ax1.set_title(f'Coverage Over Time{title_suffix}', fontsize=14, fontweight='bold', pad=20)
        ax1.legend(loc='best', fontsize=10)
        ax1.grid(True, alpha=0.3)

        # Set y-axis limits with some padding to show small changes better
        max_coverage = df[['instruction_coverage_percent', 'line_coverage_percent',
                          'method_coverage_percent', 'branch_coverage_percent']].max().max()
        min_coverage = df[['instruction_coverage_percent', 'line_coverage_percent',
                          'method_coverage_percent', 'branch_coverage_percent']].min().min()

        # Add padding to see small changes better
        padding = (max_coverage - min_coverage) * 0.1
        if padding < 1:  # Minimum padding of 1%
            padding = 1

        ax1.set_ylim(max(0, min_coverage - padding), max_coverage + padding)

        # Absolute counts
        ax2 = plt.subplot2grid((3, 2), (1, 0))
        if use_log_scale:
            ax2.set_xscale('log')
        ax2.plot(df_plot[time_col], df['total_instructions_covered'], 'o-', label='Instructions',
                 linewidth=2, markersize=marker_size)
        ax2.plot(df_plot[time_col], df['total_lines_covered'], 's-', label='Lines',
                 linewidth=2, markersize=marker_size)
        ax2.plot(df_plot[time_col], df['total_methods_covered'], '^-', label='Methods',
                 linewidth=2, markersize=marker_size)
        ax2.set_xlabel(time_label, fontsize=10)
        ax2.set_ylabel('Elements Covered', fontsize=10)
        ax2.set_title('Absolute Coverage Counts', fontsize=12, fontweight='bold')
        ax2.legend(fontsize=9)
        ax2.grid(True, alpha=0.3)

        # Coverage growth rate
        ax3 = plt.subplot2grid((3, 2), (1, 1))
        if use_log_scale:
            ax3.set_xscale('log')
        if len(df) > 1:
            inst_growth = df['instruction_coverage_percent'].diff()
            line_growth = df['line_coverage_percent'].diff()

            ax3.plot(df_plot[time_col][1:], inst_growth[1:], 'o-', label='Instruction Growth',
                     linewidth=2, markersize=marker_size)
            ax3.plot(df_plot[time_col][1:], line_growth[1:], 's-', label='Line Growth',
                     linewidth=2, markersize=marker_size)
            ax3.axhline(y=0, color='black', linestyle='--', alpha=0.5)
            ax3.legend(fontsize=9)
        else:
            ax3.text(0.5, 0.5, 'Need more\ndata points', ha='center', va='center',
                    transform=ax3.transAxes, fontsize=10)

        ax3.set_xlabel(time_label, fontsize=10)
        ax3.set_ylabel('Coverage Change (%)', fontsize=10)
        ax3.set_title('Coverage Growth Rate', fontsize=12, fontweight='bold')
        ax3.grid(True, alpha=0.3)

        # Instruction coverage breakdown (stacked area)
        ax4 = plt.subplot2grid((3, 2), (2, 0), colspan=2)
        if use_log_scale:
            ax4.set_xscale('log')

        ax4.fill_between(df_plot[time_col], 0, df['total_instructions_covered'],
                        alpha=0.7, label='Covered Instructions', color='green')
        ax4.fill_between(df_plot[time_col], df['total_instructions_covered'], df['total_instructions'],
                        alpha=0.7, label='Missed Instructions', color='red')

        ax4.set_xlabel(time_label, fontsize=12)
        ax4.set_ylabel('Total Instructions', fontsize=12)
        ax4.set_title('Instruction Coverage Breakdown', fontsize=12, fontweight='bold')
        ax4.legend(fontsize=10)
        ax4.grid(True, alpha=0.3)

        plt.tight_layout()

        # Add metadata text
        start_time = df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
        end_time = df['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')
        duration = df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]

        metadata_text = f"Data: {len(df)} points | Start: {start_time} | End: {end_time} | Duration: {duration}"
        if self.coverage_pattern:
            metadata_text += f" | Filter: {self.coverage_pattern.pattern}"

        fig.text(0.02, 0.02, metadata_text, fontsize=8, alpha=0.7)

        if save_plots:
            filter_suffix = f"_filtered_{re.sub(r'[^a-zA-Z0-9]', '_', self.coverage_pattern.pattern)}" if self.coverage_pattern else ""
            filename = DIR_EXPERIMENT_ROOT / f"coverage_timeline_{filter_suffix}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print_and_log(f"📊 Plot saved to {filename}")

        if show_plots:
            plt.show()

        return fig

    def generate_summary_report(self, df):
        """Generate text summary report"""
        if df.empty:
            return

        print_and_log("\n" + "="*60)
        print_and_log("📈 COVERAGE ANALYSIS SUMMARY REPORT")
        if self.coverage_pattern:
            print_and_log(f"🔍 FILTERED BY PATTERN: {self.coverage_pattern.pattern}")
        print_and_log("="*60)

        # Basic info
        total_duration = df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]
        print_and_log(f"📅 Analysis Period: {df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')} to {df['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')}")
        print_and_log(f"⏱️  Total Duration: {total_duration}")
        print_and_log(f"📊 Data Points: {len(df)}")

        # Coverage summary
        print_and_log(f"\n🎯 FINAL COVERAGE METRICS:")
        final = df.iloc[-1]
        print_and_log(f"   • Instruction Coverage: {final['instruction_coverage_percent']:.2f}%")
        print_and_log(f"   • Line Coverage: {final['line_coverage_percent']:.2f}%")
        print_and_log(f"   • Method Coverage: {final['method_coverage_percent']:.2f}%")
        print_and_log(f"   • Branch Coverage: {final['branch_coverage_percent']:.2f}%")

        # Progress analysis
        if len(df) > 1:
            initial = df.iloc[0]
            improvement_inst = final['instruction_coverage_percent'] - initial['instruction_coverage_percent']
            improvement_line = final['line_coverage_percent'] - initial['line_coverage_percent']

            print_and_log(f"\n📊 PROGRESS ANALYSIS:")
            print_and_log(f"   • Instruction Coverage Improvement: {improvement_inst:+.2f}%")
            print_and_log(f"   • Line Coverage Improvement: {improvement_line:+.2f}%")

            # Peak coverage
            max_inst = df['instruction_coverage_percent'].max()
            max_inst_time = df.loc[df['instruction_coverage_percent'].idxmax(), 'timestamp']
            print_and_log(f"   • Peak Instruction Coverage: {max_inst:.2f}% at {max_inst_time.strftime('%H:%M:%S')}")

        # Save detailed CSV
        filter_suffix = f"_filtered_{re.sub(r'[^a-zA-Z0-9]', '_', self.coverage_pattern.pattern)}" if self.coverage_pattern else ""
        csv_filename = DIR_EXPERIMENT_ROOT / f"coverage_timeline_detailed_{filter_suffix}.csv"
        df.to_csv(csv_filename, index=False)
        print_and_log(f"\n💾 Detailed data saved to: {csv_filename}")

def main():
    parser = argparse.ArgumentParser(
        description="Generate coverage vs time plots from JaCoCo dump files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage with default settings
  python3 coverage-plotter.py

  # Use different dump directory
  python3 coverage-plotter.py --dump-dir my-coverage-dumps

  # Filter coverage for specific packages
  python3 coverage-plotter.py --coverage-for "com\\.mycompany\\..*"

  # Filter for specific classes
  python3 coverage-plotter.py --coverage-for ".*Service.*"

  # Filter for multiple patterns (OR logic)
  python3 coverage-plotter.py --coverage-for "(com\\.myapp\\.)|(org\\.myorg\\.)"

  # Don't show plots interactively, just save them
  python3 coverage-plotter.py --no-show

  # Use different JaCoCo path
  python3 coverage-plotter.py --jacoco-path path/to/jacococli.jar
        """
    )

    parser.add_argument(
        '--dump-dir', '-d',
        type=str,
        default='coverage-dumps',
        help='Directory containing coverage dump files (default: coverage-dumps)'
    )

    parser.add_argument(
        '--jacoco-path',
        type=str,
        default='jacoco-0.8.13/lib/jacococli.jar',
        help='Path to JaCoCo CLI jar file (default: jacoco-0.8.13/lib/jacococli.jar)'
    )

    parser.add_argument(
        '--libs',
        type=str,
        default='lib/',
        help='Path to libraries directory for coverage processing (default: lib/)'
    )

    parser.add_argument(
        '--coverage-for',
        default='.*',
        type=str,
        help='Regex pattern to filter coverage data by GROUP, PACKAGE, or CLASS columns'
    )

    parser.add_argument(
        '--show',
        action='store_true',
        help='Don\'t display plots interactively, only save them'
    )

    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Don\'t save plots to files, only display them'
    )

    args = parser.parse_args()

    # Validate regex pattern
    if args.coverage_for:
        try:
            re.compile(args.coverage_for)
        except re.error as e:
            print_and_log(f"❌ Error: Invalid regex pattern '{args.coverage_for}': {e}")
            return 1

    # Validate paths
    if not Path(args.jacoco_path).exists():
        print_and_log(f"❌ Error: JaCoCo CLI not found at {args.jacoco_path}")
        return 1

    if not Path(args.dump_dir).exists():
        print_and_log(f"❌ Error: Dump directory not found: {args.dump_dir}")
        return 1

    if not Path(args.libs).exists():
        print_and_log(f"❌ Error: libraries directory not found: {args.libs}")
        return 1

    # Create plotter and process data
    plotter = CoveragePlotter(
        dump_dir=args.dump_dir,
        jacoco_path=args.jacoco_path,
        libs=args.libs,
        coverage_pattern=args.coverage_for
    )

    print_and_log("🚀 Starting Coverage Analysis...")
    print_and_log("="*50)

    # Process coverage dumps
    df = plotter.process_dumps()

    if df.empty:
        print_and_log("❌ No coverage data found to plot!")
        return 1

    print_and_log(f"✅ Successfully processed {len(df)} coverage snapshots")

    # Create plots
    plotter.create_plots(
        df,
        show_plots=args.show,
        save_plots=not args.no_save
    )

    # Generate summary report
    plotter.generate_summary_report(df)

    print_and_log("\n🎉 Analysis complete!")
    return 0

if __name__ == "__main__":
    exit(main())