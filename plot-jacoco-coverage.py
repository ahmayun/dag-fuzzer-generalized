#!/usr/bin/env python3

import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import os
import re
from pathlib import Path

def run_jacoco_report(dump_file, csv_file):
    """Run JaCoCo CLI to generate CSV report from dump file"""
    cmd = [
        'java', '-jar', 'jacoco-0.8.13/lib/jacococli.jar', 'report', dump_file,
        '--classfiles', 'lib-spark-only/',
        '--csv', csv_file
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print(f"✓ Generated report for {dump_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to generate report for {dump_file}: {e}")
        return False

def extract_test_number(filename):
    """Extract test number from filename like 'coverage-dump-test-5.exec'"""
    match = re.search(r'test-(\d+)', filename)
    return int(match.group(1)) if match else 0

def process_coverage_dumps():
    """Process all coverage dump files and create summary"""

    # Create output directory
    Path('coverage-reports').mkdir(exist_ok=True)

    # Find all coverage dump files
    dump_files = list(Path('.').glob('coverage-dump-test-*.exec'))
    dump_files.sort(key=lambda x: extract_test_number(x.name))

    print(f"Found {len(dump_files)} coverage dump files")

    # Process each dump file
    summary_data = []

    for dump_file in dump_files:
        test_num = extract_test_number(dump_file.name)
        csv_file = f'coverage-reports/coverage-report-{test_num}.csv'

        print(f"Processing {dump_file.name}...")

        if run_jacoco_report(str(dump_file), csv_file):
            # Read the generated CSV and calculate totals
            try:
                df = pd.read_csv(csv_file)

                # Calculate totals
                total_inst_covered = df['INSTRUCTION_COVERED'].sum()
                total_inst_missed = df['INSTRUCTION_MISSED'].sum()
                total_lines_covered = df['LINE_COVERED'].sum()
                total_lines_missed = df['LINE_MISSED'].sum()
                total_methods_covered = df['METHOD_COVERED'].sum()
                total_methods_missed = df['METHOD_MISSED'].sum()

                # Calculate percentages
                total_inst = total_inst_covered + total_inst_missed
                total_lines = total_lines_covered + total_lines_missed
                total_methods = total_methods_covered + total_methods_missed

                inst_pct = (total_inst_covered / total_inst * 100) if total_inst > 0 else 0
                line_pct = (total_lines_covered / total_lines * 100) if total_lines > 0 else 0
                method_pct = (total_methods_covered / total_methods * 100) if total_methods > 0 else 0

                summary_data.append({
                    'test_point': test_num,
                    'instruction_coverage_percent': inst_pct,
                    'line_coverage_percent': line_pct,
                    'method_coverage_percent': method_pct,
                    'total_instructions_covered': total_inst_covered,
                    'total_lines_covered': total_lines_covered,
                    'total_methods_covered': total_methods_covered,
                    'total_instructions': total_inst,
                    'total_lines': total_lines,
                    'total_methods': total_methods
                })

            except Exception as e:
                print(f"✗ Error processing {csv_file}: {e}")

    return pd.DataFrame(summary_data).sort_values('test_point')

def create_coverage_plots(df):
    """Create coverage plots"""

    # Create subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Spark Coverage Over Time', fontsize=16, fontweight='bold')

    # Plot 1: Coverage Percentages
    ax1.plot(df['test_point'], df['instruction_coverage_percent'], 'b-o', label='Instruction Coverage', linewidth=2, markersize=6)
    ax1.plot(df['test_point'], df['line_coverage_percent'], 'r-s', label='Line Coverage', linewidth=2, markersize=6)
    ax1.plot(df['test_point'], df['method_coverage_percent'], 'g-^', label='Method Coverage', linewidth=2, markersize=6)
    ax1.set_xlabel('Test Point')
    ax1.set_ylabel('Coverage Percentage (%)')
    ax1.set_title('Coverage Percentages Over Time')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(0, max(df[['instruction_coverage_percent', 'line_coverage_percent', 'method_coverage_percent']].max()) * 1.1)

    # Plot 2: Absolute Covered Elements
    ax2.plot(df['test_point'], df['total_instructions_covered'], 'b-o', label='Instructions Covered', linewidth=2, markersize=6)
    ax2.plot(df['test_point'], df['total_lines_covered'], 'r-s', label='Lines Covered', linewidth=2, markersize=6)
    ax2.plot(df['test_point'], df['total_methods_covered'], 'g-^', label='Methods Covered', linewidth=2, markersize=6)
    ax2.set_xlabel('Test Point')
    ax2.set_ylabel('Elements Covered (Count)')
    ax2.set_title('Absolute Coverage Counts Over Time')
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    # Plot 3: Instruction Coverage Detail
    ax3.bar(df['test_point'], df['total_instructions_covered'], label='Covered', color='green', alpha=0.7, width=0.6)
    ax3.bar(df['test_point'], df['total_instructions'] - df['total_instructions_covered'],
            bottom=df['total_instructions_covered'], label='Missed', color='red', alpha=0.7, width=0.6)
    ax3.set_xlabel('Test Point')
    ax3.set_ylabel('Instructions Count')
    ax3.set_title('Instruction Coverage Breakdown')
    ax3.legend()
    ax3.grid(True, alpha=0.3)

    # Plot 4: Coverage Growth Rate
    if len(df) > 1:
        inst_growth = df['instruction_coverage_percent'].diff()
        line_growth = df['line_coverage_percent'].diff()
        method_growth = df['method_coverage_percent'].diff()

        ax4.plot(df['test_point'][1:], inst_growth[1:], 'b-o', label='Instruction Growth', linewidth=2, markersize=6)
        ax4.plot(df['test_point'][1:], line_growth[1:], 'r-s', label='Line Growth', linewidth=2, markersize=6)
        ax4.plot(df['test_point'][1:], method_growth[1:], 'g-^', label='Method Growth', linewidth=2, markersize=6)
        ax4.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax4.set_xlabel('Test Point')
        ax4.set_ylabel('Coverage Change (%)')
        ax4.set_title('Coverage Growth Rate')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
    else:
        ax4.text(0.5, 0.5, 'Need more data points\nfor growth rate',
                ha='center', va='center', transform=ax4.transAxes, fontsize=12)
        ax4.set_title('Coverage Growth Rate')

    plt.tight_layout()
    plt.savefig('spark_coverage_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()

def main():
    print("🚀 Starting Spark Coverage Analysis...")
    print("=" * 50)

    # Process coverage dumps
    df = process_coverage_dumps()

    if df.empty:
        print("❌ No coverage data found!")
        return

    print(f"\n📊 Successfully processed {len(df)} test points")
    print("\nSummary Statistics:")
    print(df[['test_point', 'instruction_coverage_percent', 'line_coverage_percent', 'method_coverage_percent']].to_string(index=False, float_format='%.2f'))

    # Save summary to CSV
    df.to_csv('coverage_summary.csv', index=False, float_format='%.2f')
    print(f"\n💾 Summary saved to coverage_summary.csv")

    # Create plots
    print("\n📈 Creating coverage plots...")
    create_coverage_plots(df)
    print("✅ Plots saved to spark_coverage_analysis.png")

    # Print some insights
    if len(df) > 1:
        final_inst_cov = df['instruction_coverage_percent'].iloc[-1]
        initial_inst_cov = df['instruction_coverage_percent'].iloc[0]
        improvement = final_inst_cov - initial_inst_cov

        print(f"\n🎯 Coverage Insights:")
        print(f"   • Initial instruction coverage: {initial_inst_cov:.2f}%")
        print(f"   • Final instruction coverage: {final_inst_cov:.2f}%")
        print(f"   • Total improvement: {improvement:+.2f}%")

        max_cov = df['instruction_coverage_percent'].max()
        max_point = df.loc[df['instruction_coverage_percent'].idxmax(), 'test_point']
        print(f"   • Peak coverage: {max_cov:.2f}% at test point {max_point}")

if __name__ == "__main__":
    main()