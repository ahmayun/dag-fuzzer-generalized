"""
USAGE EXAMPLES:
    python coverage-analysis-scripts/process-coverage-to-csv.py --mode dump2csv --results-dir cluster-results/dagfuzz/flink-python --output-dir cluster-results-processed/dagfuzz/flink-python --jacoco-path jacoco-0.8.13/lib/jacococli.jar --libs-path lib-flink-filtered-java-11-only/
    python coverage-analysis-scripts/process-coverage-to-csv.py --mode csv2plot --reports-dir coverage-reports/20250926_011102

    python coverage-analysis-scripts/process-coverage-to-csv.py --mode plot_coverage_comparison \
        --report-dirs \
            "cluster-results-processed/dagfuzz/flink-python/headnode/dump-20250927_123145" \
            "cluster-results-processed/dagfuzz/flink-python/headnode/dump-20250927_123146" \
            "cluster-results-processed/dagfuzz/flink-python/headnode/dump-20250927_123147" \
        --metric instruction_coverage_percent \
        --save-path ".tmp-plots/coverage.png"

    python script.py --mode plot_all_metrics \
        --report-dirs \
            "cluster-results-processed/dagfuzz/flink-python/headnode/dump-20250927_123145" \
            "cluster-results-processed/dagfuzz/flink-python/headnode/dump-20250927_123146" \
            "cluster-results-processed/dagfuzz/flink-python/headnode/dump-20250927_123147" \
        --save-dir "plots"
"""

import subprocess
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import argparse
import numpy as np

def print_and_log(*args, **kwargs):
    print(*args, **kwargs)

def generate_jacoco_report(jacoco_path, libs_path, dump_file, out_csv_file):
    """Run JaCoCo CLI to generate CSV report from dump file"""
    cmd = [
        'java', '-jar', jacoco_path, 'report', str(dump_file),
        '--classfiles', libs_path,
        '--csv', str(out_csv_file)
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print_and_log(f"✓ Generated report for {dump_file.name}")
        return True
    except subprocess.CalledProcessError as e:
        print_and_log(f"✗ Failed to generate report for {dump_file.name}: {e}")
        return False


def _extract_timestamp_from_filename(filename):
    """Extract timestamp from filename like 'coverage-dump_20241124_143022.exec'"""
    match = re.search(r'coverage-dump_(\d{8}_\d{6})\.exec', filename)
    if match:
        timestamp_str = match.group(1)
        return datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
    return None


def _extract_timestamp_from_report_filename(filename):
    """Extract timestamp from report filename (report_YYYYMMDD_HHMMSS.csv)"""
    match = re.search(r'report_(\d{8})_(\d{6})\.csv', filename)
    if match:
        try:
            date_str = match.group(1)
            time_str = match.group(2)
            return datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
        except ValueError:
            return None
    return None


def _filter_coverage_data(df, coverage_pattern):
    """Filter coverage data by pattern"""
    if coverage_pattern is None:
        return df

    filter_columns = ['GROUP', 'PACKAGE', 'CLASS']

    # Check which columns exist in the dataframe
    existing_columns = [col for col in filter_columns if col in df.columns]

    if not existing_columns:
        print_and_log(f"⚠️  Warning: No filterable columns found ({filter_columns}). Using all data.")
        return df

    # Combine all matching rows across available columns
    mask = df[existing_columns].apply(
        lambda row: any(coverage_pattern.search(str(val)) for val in row if pd.notna(val)),
        axis=1
    )

    return df[mask]


def _compile_coverage_pattern(libs_regex_filter):
    """Compile regex pattern for filtering coverage data"""
    return re.compile(libs_regex_filter) if libs_regex_filter else None


def _get_sorted_files(directory, pattern, timestamp_extractor):
    """Get and sort files by timestamp using provided extractor function"""
    files = list(directory.glob(pattern))

    if not files:
        print_and_log(f"❌ No files found in {directory}")
        print_and_log(f"   Looking for files matching pattern: {pattern}")
        return []

    files.sort(key=lambda x: timestamp_extractor(x.name) or datetime.min)
    return files


def _get_sorted_dump_files(dump_dir):
    """Get and sort coverage dump files by timestamp"""
    return _get_sorted_files(dump_dir, 'coverage-dump_*.exec', _extract_timestamp_from_filename)


def _log_processing_info(files, coverage_pattern, file_type="coverage dump"):
    """Log information about files to process and filtering"""
    print_and_log(f"📁 Found {len(files)} {file_type} files")
    if coverage_pattern:
        print_and_log(f"🔍 Will filter coverage data using pattern: '{coverage_pattern.pattern}'")


def _get_report_filepath(reports_dir, timestamp):
    """Generate CSV report filepath for a given timestamp"""
    return reports_dir / f"report_{timestamp.strftime('%Y%m%d_%H%M%S')}.csv"


def _process_single_csv_report(csv_file, timestamp, coverage_pattern):
    """Process a CSV report file and return coverage metrics"""
    try:
        df = pd.read_csv(csv_file)
        df_filtered = _filter_coverage_data(df, coverage_pattern)
        metrics = _calculate_coverage_metrics(df_filtered)

        return {
            'timestamp': timestamp,
            **metrics
        }
    except Exception as e:
        print_and_log(f"✗ Error processing {csv_file}: {e}")
        return None


def _process_single_dump(jacoco_path, libs_path, dump_file, csv_file, timestamp, coverage_pattern):
    """Process a single dump file and return coverage metrics"""
    if not generate_jacoco_report(jacoco_path, libs_path, dump_file, csv_file):
        return None

    return _process_single_csv_report(csv_file, timestamp, coverage_pattern)


def _process_all_dumps(dump_files, jacoco_path, libs_path, reports_dir, coverage_pattern):
    """Process all dump files and collect coverage data"""
    coverage_data = []

    for i, dump_file in enumerate(dump_files):
        timestamp = _extract_timestamp_from_filename(dump_file.name)
        if not timestamp:
            print_and_log(f"⚠️  Skipping {dump_file.name} - couldn't parse timestamp")
            continue

        print_and_log(f"🔄 Processing {dump_file.name} ({i+1}/{len(dump_files)})...")

        csv_file = _get_report_filepath(reports_dir, timestamp)
        coverage_entry = _process_single_dump(
            jacoco_path, libs_path, dump_file, csv_file, timestamp, coverage_pattern
        )

        if coverage_entry:
            coverage_data.append(coverage_entry)

    return coverage_data


def _process_all_report_files(report_files, coverage_pattern):
    """Process all existing CSV report files and collect coverage data"""
    coverage_data = []

    for i, report_file in enumerate(report_files):
        timestamp = _extract_timestamp_from_report_filename(report_file.name)
        if not timestamp:
            print_and_log(f"⚠️  Skipping {report_file.name} - couldn't parse timestamp")
            continue

        print_and_log(f"🔄 Processing {report_file.name} ({i+1}/{len(report_files)})...")

        coverage_entry = _process_single_csv_report(report_file, timestamp, coverage_pattern)

        if coverage_entry:
            coverage_data.append(coverage_entry)

    return coverage_data


def _calculate_coverage_metrics(df_filtered):
    """Calculate all coverage metrics from filtered data"""
    totals = _sum_coverage_columns(df_filtered)
    percentages = _calculate_coverage_percentages(totals)

    return {
        'instruction_coverage_percent': percentages['instruction'],
        'line_coverage_percent': percentages['line'],
        'method_coverage_percent': percentages['method'],
        'branch_coverage_percent': percentages['branch'],
        'total_instructions_covered': totals['inst_covered'],
        'total_lines_covered': totals['lines_covered'],
        'total_methods_covered': totals['methods_covered'],
        'total_branches_covered': totals['branches_covered'],
        'total_instructions': totals['inst_total'],
        'total_lines': totals['lines_total'],
        'total_methods': totals['methods_total'],
        'total_branches': totals['branches_total']
    }


def _sum_coverage_columns(df):
    """Sum all coverage columns from the dataframe"""
    return {
        'inst_covered': df['INSTRUCTION_COVERED'].sum(),
        'inst_missed': df['INSTRUCTION_MISSED'].sum(),
        'lines_covered': df['LINE_COVERED'].sum(),
        'lines_missed': df['LINE_MISSED'].sum(),
        'methods_covered': df['METHOD_COVERED'].sum(),
        'methods_missed': df['METHOD_MISSED'].sum(),
        'branches_covered': df['BRANCH_COVERED'].sum(),
        'branches_missed': df['BRANCH_MISSED'].sum(),
        'inst_total': df['INSTRUCTION_COVERED'].sum() + df['INSTRUCTION_MISSED'].sum(),
        'lines_total': df['LINE_COVERED'].sum() + df['LINE_MISSED'].sum(),
        'methods_total': df['METHOD_COVERED'].sum() + df['METHOD_MISSED'].sum(),
        'branches_total': df['BRANCH_COVERED'].sum() + df['BRANCH_MISSED'].sum()
    }


def _calculate_coverage_percentages(totals):
    """Calculate coverage percentages from totals"""
    def safe_percentage(covered, total):
        return (covered / total * 100) if total > 0 else 0

    return {
        'instruction': safe_percentage(totals['inst_covered'], totals['inst_total']),
        'line': safe_percentage(totals['lines_covered'], totals['lines_total']),
        'method': safe_percentage(totals['methods_covered'], totals['methods_total']),
        'branch': safe_percentage(totals['branches_covered'], totals['branches_total'])
    }


def _create_coverage_dataframe(coverage_data):
    """Create and enrich the final coverage dataframe"""
    df_coverage = pd.DataFrame(coverage_data)
    df_coverage = df_coverage.sort_values('timestamp')
    df_coverage = _add_time_elapsed_columns(df_coverage)
    return df_coverage


def _add_time_elapsed_columns(df):
    """Add time elapsed columns relative to start time"""
    start_time = df['timestamp'].iloc[0]
    df['time_elapsed_minutes'] = (df['timestamp'] - start_time).dt.total_seconds() / 60
    df['time_elapsed_hours'] = df['time_elapsed_minutes'] / 60
    return df


def process_jacoco_dumps(jacoco_path, libs_path, libs_regex_filter, dump_dir_path, reports_dir_path):
    """Process all coverage dump files and create time-series data"""

    dump_dir = Path(dump_dir_path)
    reports_dir = Path(reports_dir_path)
    reports_dir.mkdir(parents=True, exist_ok=True)

    coverage_pattern = _compile_coverage_pattern(libs_regex_filter)

    dump_files = _get_sorted_dump_files(dump_dir)
    if not dump_files:
        return pd.DataFrame()

    _log_processing_info(dump_files, coverage_pattern)

    coverage_data = _process_all_dumps(
        dump_files, jacoco_path, libs_path, reports_dir, coverage_pattern
    )

    if not coverage_data:
        print_and_log("❌ No valid coverage data found!")
        return pd.DataFrame()

    return _create_coverage_dataframe(coverage_data)


def process_all_reports(reports_dir_path, libs_regex_filter=None):
    """Process existing CSV report files and create time-series data without reprocessing dumps"""

    reports_dir = Path(reports_dir_path)

    coverage_pattern = _compile_coverage_pattern(libs_regex_filter)

    report_files = _get_sorted_files(reports_dir, 'report_*.csv', _extract_timestamp_from_report_filename)
    if not report_files:
        return pd.DataFrame()

    _log_processing_info(report_files, coverage_pattern, file_type="report")

    coverage_data = _process_all_report_files(report_files, coverage_pattern)

    if not coverage_data:
        print_and_log("❌ No valid coverage data found in reports!")
        return pd.DataFrame()

    return _create_coverage_dataframe(coverage_data)


def process_results_dir(results_dir, output_dir, jacoco_path, libs_path, libs_regex_filter=None):
    """
    Recursively traverse results_dir to find directories with timestamped coverage dumps.
    Process each found directory and create mirrored structure in output_dir with CSV reports.

    Args:
        results_dir: Root directory to search for coverage dumps
        output_dir: Root directory where CSV reports will be saved
        jacoco_path: Path to jacococli.jar
        libs_path: Path to class files for coverage analysis
        libs_regex_filter: Optional regex pattern to filter coverage data

    Returns:
        dict: Mapping of dump directories to their corresponding report directories
    """
    results_dir = Path(results_dir)
    output_dir = Path(output_dir)

    if not results_dir.exists():
        print_and_log(f"❌ Results directory does not exist: {results_dir}")
        return {}

    # Find all directories containing coverage dump files
    dump_dirs = _find_dump_directories(results_dir)

    if not dump_dirs:
        print_and_log(f"❌ No directories with coverage dumps found in {results_dir}")
        return {}

    print_and_log(f"📂 Found {len(dump_dirs)} directories with coverage dumps")

    # Process each dump directory
    processed_dirs = {}

    for i, dump_dir in enumerate(dump_dirs, 1):
        # Calculate relative path from results_dir to dump_dir
        rel_path = dump_dir.relative_to(results_dir)

        # Create corresponding output directory
        reports_dir = output_dir / rel_path

        print_and_log(f"\n{'='*60}")
        print_and_log(f"📍 Processing directory {i}/{len(dump_dirs)}: {rel_path}")
        print_and_log(f"{'='*60}")

        # Process the dumps in this directory
        df = process_jacoco_dumps(
            jacoco_path,
            libs_path,
            libs_regex_filter,
            dump_dir,
            reports_dir
        )

        if not df.empty:
            processed_dirs[str(dump_dir)] = str(reports_dir)
            print_and_log(f"✓ Successfully processed {len(df)} coverage snapshots")
        else:
            print_and_log(f"⚠️  No valid coverage data extracted from {rel_path}")

    print_and_log(f"\n{'='*60}")
    print_and_log(f"✅ Completed processing {len(processed_dirs)}/{len(dump_dirs)} directories")
    print_and_log(f"📁 Reports saved to: {output_dir}")
    print_and_log(f"{'='*60}\n")

    return processed_dirs


def _find_dump_directories(root_dir):
    """
    Recursively find all directories containing coverage dump files.

    Args:
        root_dir: Root directory to search

    Returns:
        list: List of Path objects for directories containing dump files
    """
    dump_dirs = []

    for path in root_dir.rglob('coverage-dump_*.exec'):
        dump_dir = path.parent

        # Only add each directory once (avoid duplicates if multiple dumps in same dir)
        if dump_dir not in dump_dirs:
            dump_dirs.append(dump_dir)

    # Sort by path for consistent processing order
    dump_dirs.sort()

    return dump_dirs

def process_multiple_reports(reports_dir_paths, libs_regex_filter=None, interpolate=True):
    """
    Process multiple CSV report directories and create aggregated time-series data with statistics.

    Args:
        reports_dir_paths: List of paths to report directories
        libs_regex_filter: Optional regex pattern to filter coverage data
        interpolate: If True, interpolate values to common time points

    Returns:
        DataFrame with mean, std, min, max coverage metrics across all runs
    """
    if isinstance(reports_dir_paths, (str, Path)):
        reports_dir_paths = [reports_dir_paths]

    print_and_log(f"📊 Processing {len(reports_dir_paths)} report directories")

    # Collect dataframes from each directory
    dfs = []
    for i, path in enumerate(reports_dir_paths, 1):
        print_and_log(f"\n🔄 Processing directory {i}/{len(reports_dir_paths)}: {path}")
        df = process_all_reports(path, libs_regex_filter)

        if not df.empty:
            dfs.append(df)
            print_and_log(f"✓ Loaded {len(df)} coverage snapshots")
        else:
            print_and_log(f"⚠️  No data found in {path}")

    if not dfs:
        print_and_log("❌ No valid data found in any directory!")
        return pd.DataFrame()

    print_and_log(f"\n✅ Successfully loaded data from {len(dfs)} directories")

    if interpolate:
        return _aggregate_with_interpolation(dfs)
    else:
        return _aggregate_without_interpolation(dfs)


def _aggregate_with_interpolation(dfs):
    """
    Interpolate all runs to common time points and calculate statistics.
    """
    # Find common time range across all runs
    min_time = max(df['time_elapsed_minutes'].min() for df in dfs)
    max_time = min(df['time_elapsed_minutes'].max() for df in dfs)

    # Create common time points (every minute within overlap range)
    common_times = np.arange(min_time, max_time + 1, 1.0)

    print_and_log(f"📈 Interpolating to {len(common_times)} time points ({min_time:.1f} to {max_time:.1f} minutes)")

#   ['instruction_coverage_percent', 'line_coverage_percent', 'method_coverage_percent', 'branch_coverage_percent', 'total_instructions_covered', 'total_lines_covered', 'total_methods_covered', 'total_branches_covered', 'total_instructions', 'total_lines', 'total_methods', 'total_branches', 'time_elapsed_minutes', 'time_elapsed_hours']
#     coverage_metrics = ['instruction_coverage_percent', 'total_instructions_covered', 'total_lines_covered']
    coverage_metrics = ['total_instructions_covered', 'total_branches_covered', 'total_methods_covered']


    interpolated_data = {metric: [] for metric in coverage_metrics}

    for df in dfs:
        for metric in coverage_metrics:
            # Interpolate this metric for this run
            interp_values = np.interp(
                common_times,
                df['time_elapsed_minutes'],
                df[metric]
            )
            interpolated_data[metric].append(interp_values)

    # Calculate statistics across runs
    result_data = {'time_elapsed_minutes': common_times}

    for metric in coverage_metrics:
        values_array = np.array(interpolated_data[metric])  # shape: (num_runs, num_time_points)

        result_data[f'{metric}_mean'] = np.mean(values_array, axis=0)
        result_data[f'{metric}_std'] = np.std(values_array, axis=0)
        result_data[f'{metric}_min'] = np.min(values_array, axis=0)
        result_data[f'{metric}_max'] = np.max(values_array, axis=0)
        result_data[f'{metric}_sem'] = np.std(values_array, axis=0) / np.sqrt(len(dfs))  # Standard error

    df_result = pd.DataFrame(result_data)
    df_result['time_elapsed_hours'] = df_result['time_elapsed_minutes'] / 60
    df_result['num_runs'] = len(dfs)

    return df_result


def _aggregate_without_interpolation(dfs):
    """
    Concatenate all runs without interpolation, grouping by time buckets.
    """
    # Add run identifier to each dataframe
    for i, df in enumerate(dfs):
        df['run_id'] = i

    # Concatenate all dataframes
    df_combined = pd.concat(dfs, ignore_index=True)

    # Round times to nearest minute for grouping
    df_combined['time_bucket'] = df_combined['time_elapsed_minutes'].round(0)

    # Calculate statistics for each time bucket
    coverage_metrics = ['instruction_coverage_percent', 'line_coverage_percent',
                       'method_coverage_percent', 'branch_coverage_percent']

    agg_dict = {}
    for metric in coverage_metrics:
        agg_dict[metric] = ['mean', 'std', 'min', 'max', 'sem']

    df_stats = df_combined.groupby('time_bucket')[coverage_metrics].agg(['mean', 'std', 'min', 'max', 'sem'])
    df_stats.columns = ['_'.join(col).strip() for col in df_stats.columns.values]
    df_stats = df_stats.reset_index()
    df_stats.rename(columns={'time_bucket': 'time_elapsed_minutes'}, inplace=True)
    df_stats['time_elapsed_hours'] = df_stats['time_elapsed_minutes'] / 60
    df_stats['num_runs'] = len(dfs)

    return df_stats


def plot_coverage_comparison(df_stats_list, metric='instruction_coverage_percent',
                            show_bands=True, save_path=None, smoothing_window=None,
                            xmin=None, xmax=None, ymin=None, ymax=None,
                            labels=None, colors=None):
    """
    Plot coverage metrics over time with error bands and optional smoothing.

    Args:
        df_stats_list: Single DataFrame or list of DataFrames from process_multiple_reports()
        metric: Coverage metric to plot (without _mean suffix)
        show_bands: If True, show std deviation bands
        save_path: Optional path to save the plot
        smoothing_window: Window size for rolling average smoothing (None for no smoothing)
                         Recommended: 5-10 for minute-level data
        xmin: Minimum x-axis value (time in hours)
        xmax: Maximum x-axis value (time in hours)
        ymin: Minimum y-axis value
        ymax: Maximum y-axis value
        labels: List of labels for each DataFrame (default: 'Dataset 1', 'Dataset 2', etc.)
        colors: List of colors for each DataFrame (default: auto-generated)
    """
    import matplotlib.pyplot as plt

    # Handle single DataFrame input
    if not isinstance(df_stats_list, list):
        df_stats_list = [df_stats_list]

    # Set default labels
    if labels is None:
        labels = [f'Dataset {i+1}' for i in range(len(df_stats_list))]

    # Set default colors
    if colors is None:
        default_colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E', '#BC4B51']
        colors = default_colors[:len(df_stats_list)]
        if len(df_stats_list) > len(default_colors):
            # Generate additional colors if needed
            import matplotlib.cm as cm
            cmap = cm.get_cmap('tab10')
            colors = [cmap(i % 10) for i in range(len(df_stats_list))]

    fig, ax = plt.subplots(figsize=(12, 6))

    time_col = 'time_elapsed_hours'
    mean_col = f'{metric}_mean'
    std_col = f'{metric}_std'
    min_col = f'{metric}_min'
    max_col = f'{metric}_max'

    # Plot each DataFrame
    for idx, (df_stats, label, color) in enumerate(zip(df_stats_list, labels, colors)):
        # Apply smoothing if requested
        if smoothing_window:
            df_plot = df_stats.copy()
            df_plot[mean_col] = df_plot[mean_col].rolling(window=smoothing_window, center=True, min_periods=1).mean()
            df_plot[std_col] = df_plot[std_col].rolling(window=smoothing_window, center=True, min_periods=1).mean()
            df_plot[min_col] = df_plot[min_col].rolling(window=smoothing_window, center=True, min_periods=1).mean()
            df_plot[max_col] = df_plot[max_col].rolling(window=smoothing_window, center=True, min_periods=1).mean()
        else:
            df_plot = df_stats

        # Plot mean line
        ax.plot(df_plot[time_col], df_plot[mean_col],
                linewidth=2, label=f'{label} (Mean, n={df_stats["num_runs"].iloc[0]})',
                color=color)

        if show_bands:
            # Plot standard deviation bands
            ax.fill_between(
                df_plot[time_col],
                df_plot[mean_col] - df_plot[std_col],
                df_plot[mean_col] + df_plot[std_col],
                alpha=0.3, color=color, label=f'{label} (±1 Std Dev)'
            )

            # Plot min/max range
            ax.fill_between(
                df_plot[time_col],
                df_plot[min_col],
                df_plot[max_col],
                alpha=0.15, color=color, label=f'{label} (Min-Max)'
            )

    # Set axis limits
    if xmin is not None or xmax is not None:
        ax.set_xlim(left=xmin, right=xmax)

    if ymin is not None or ymax is not None:
        ax.set_ylim(bottom=ymin, top=ymax)
    elif ymin is None and ymax is None:
        ax.set_ylim(bottom=0)  # Default behavior

    # Formatting
    metric_name = metric.replace('_', ' ').title()
    ax.set_xlabel('Time Elapsed (hours)', fontsize=12)

    # Determine if metric is a percentage
    if 'percent' in metric:
        ax.set_ylabel(f'{metric_name} (%)', fontsize=12)
    else:
        ax.set_ylabel(f'{metric_name}', fontsize=12)

    title = f'{metric_name} Over Time'
    if smoothing_window:
        title += f' [smoothed: window={smoothing_window}]'
    ax.set_title(title, fontsize=14, fontweight='bold')

    ax.grid(True, alpha=0.3, linestyle='--')
    ax.legend(loc='best', fontsize=9)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        # Save all dataframes to separate CSVs
        for idx, (df_stats, label) in enumerate(zip(df_stats_list, labels)):
            csv_path = f"{save_path}.csv"
            df_stats.to_csv(csv_path, index=False)
        print(f"📊 Plot saved to {save_path}")

    plt.show()


def plot_all_metrics(df_stats, show_bands=True, save_dir=None, smoothing_window=None,
                     xmin=None, xmax=None, ymin=None, ymax=None):
    """
    Plot all coverage metrics in a 2x2 grid with optional smoothing.

    Args:
        df_stats: DataFrame from process_multiple_reports()
        show_bands: If True, show std deviation bands
        save_dir: Optional directory to save individual plots
        smoothing_window: Window size for rolling average smoothing (None for no smoothing)
        xmin: Minimum x-axis value (time in hours)
        xmax: Maximum x-axis value (time in hours)
        ymin: Minimum y-axis value
        ymax: Maximum y-axis value
    """
    import matplotlib.pyplot as plt

    metrics = [
        'instruction_coverage_percent',
        'line_coverage_percent',
        'method_coverage_percent',
        'branch_coverage_percent'
    ]

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    axes = axes.flatten()

    time_col = 'time_elapsed_hours'

    # Apply smoothing if requested
    if smoothing_window:
        df_plot = df_stats.copy()
        for metric in metrics:
            for suffix in ['_mean', '_std', '_min', '_max']:
                col = f'{metric}{suffix}'
                df_plot[col] = df_plot[col].rolling(window=smoothing_window, center=True, min_periods=1).mean()
    else:
        df_plot = df_stats

    for idx, metric in enumerate(metrics):
        ax = axes[idx]
        mean_col = f'{metric}_mean'
        std_col = f'{metric}_std'
        min_col = f'{metric}_min'
        max_col = f'{metric}_max'

        # Plot mean line
        ax.plot(df_plot[time_col], df_plot[mean_col],
                linewidth=2, label='Mean', color='#2E86AB')

        if show_bands:
            # Standard deviation bands
            ax.fill_between(
                df_plot[time_col],
                df_plot[mean_col] - df_plot[std_col],
                df_plot[mean_col] + df_plot[std_col],
                alpha=0.3, color='#2E86AB', label='±1 Std Dev'
            )

            # Min/max range
            ax.fill_between(
                df_plot[time_col],
                df_plot[min_col],
                df_plot[max_col],
                alpha=0.15, color='#2E86AB', label='Min-Max Range'
            )

        # Set axis limits
        if xmin is not None or xmax is not None:
            ax.set_xlim(left=xmin, right=xmax)

        if ymin is not None or ymax is not None:
            ax.set_ylim(bottom=ymin, top=ymax)
        elif ymin is None and ymax is None:
            ax.set_ylim(bottom=0)  # Default behavior

        # Formatting
        metric_name = metric.replace('_', ' ').title()
        ax.set_xlabel('Time Elapsed (hours)', fontsize=11)
        ax.set_ylabel(f'{metric_name} (%)', fontsize=11)
        ax.set_title(metric_name, fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(loc='best', fontsize=9)

    title = f'Coverage Metrics Over Time (n={df_stats["num_runs"].iloc[0]} runs)'
    if smoothing_window:
        title += f' [smoothed: window={smoothing_window}]'
    fig.suptitle(title, fontsize=16, fontweight='bold', y=1.00)

    plt.tight_layout()

    if save_dir:
        save_dir = Path(save_dir)
        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / 'coverage_all_metrics.png'
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print_and_log(f"📊 Plot saved to {save_path}")

    plt.show()

def run_dump2csv(args):
    """Process JaCoCo .exec files and generate coverage reports."""
    processed = process_results_dir(
        results_dir=args.results_dir,
        output_dir=args.output_dir,
        jacoco_path=args.jacoco_path,
        libs_path=args.libs_path,
        libs_regex_filter=args.libs_regex_filter
    )

    # View what was processed
    for dump_dir, report_dir in processed.items():
        print(f"{dump_dir} -> {report_dir}")


def run_csv2plot(args):
    """Process JaCoCo reports and generate coverage dataframe."""
    df = process_all_reports(
        args.reports_dir,
        libs_regex_filter=args.libs_regex_filter
    )

    print(df.head(args.show_rows))


def run_plot_coverage_comparison(args):
    """Plot coverage comparison for multiple report directories."""
    # Get aggregated statistics with interpolation
    df_stats = process_multiple_reports(
        args.report_dirs,
        libs_regex_filter=args.libs_regex_filter
    )

    print(df_stats.head(args.show_rows))
    print(df_stats.columns)

    # Plot single metric
    plot_coverage_comparison(
        df_stats,
        metric=args.metric,
        save_path=args.save_path,
        smoothing_window=args.smoothing_window,
        xmin=args.xmin,
        xmax=args.xmax,
        ymin=args.ymin,
        ymax=args.ymax
    )


def run_plot_all_metrics(args):
    """Plot all metrics in a grid for multiple report directories."""
    # Get aggregated statistics with interpolation
    df_stats = process_multiple_reports(
        args.report_dirs,
        libs_regex_filter=args.libs_regex_filter
    )

    print(df_stats.head(args.show_rows))

    # Plot all metrics in grid
    plot_all_metrics(df_stats, save_dir=args.save_dir)


def create_dump2csv_parser():
    """Create argument parser for dump2csv mode."""
    parser = argparse.ArgumentParser(
        description="Process JaCoCo results directory and generate coverage reports"
    )

    parser.add_argument("--mode", choices=["dump2csv", "csv2plot", "plot_coverage_comparison", "plot_all_metrics", "stats2plot"])

    parser.add_argument(
        "--results-dir",
        required=True,
        help="Path to the results directory containing .exec files"
    )

    parser.add_argument(
        "--output-dir",
        required=True,
        help="Path to the output directory for processed reports"
    )

    parser.add_argument(
        "--jacoco-path",
        required=True,
        help="Path to the JaCoCo CLI jar file"
    )

    parser.add_argument(
        "--libs-path",
        required=True,
        help="Path to the libraries directory"
    )

    parser.add_argument(
        "--libs-regex-filter",
        default=None,
        help="Optional regex filter for library files (default: None)"
    )

    return parser


def create_csv2plot_parser():
    """Create argument parser for csv2plot mode."""
    parser = argparse.ArgumentParser(
        description="Process all JaCoCo reports and generate coverage dataframe"
    )

    parser.add_argument("--mode", choices=["dump2csv", "csv2plot", "plot_coverage_comparison", "plot_all_metrics", "stats2plot"])


    parser.add_argument(
        "--reports-dir",
        required=True,
        help="Path to the directory containing JaCoCo reports"
    )

    parser.add_argument(
        "--libs-regex-filter",
        default=None,
        help="Optional regex filter for library files (default: None)"
    )

    parser.add_argument(
        "--show-rows",
        type=int,
        default=5,
        help="Number of rows to display from the dataframe (default: 5)"
    )

    return parser


def create_plot_coverage_comparison_parser():
    """Create argument parser for plot_coverage_comparison mode."""
    parser = argparse.ArgumentParser(
        description="Plot coverage comparison for multiple report directories"
    )

    parser.add_argument("--mode", choices=["dump2csv", "csv2plot", "plot_coverage_comparison", "plot_all_metrics", "stats2plot"])

    parser.add_argument(
        "--report-dirs",
        nargs='+',
        required=True,
        help="List of report directory paths to process"
    )

    parser.add_argument(
        "--libs-regex-filter",
        default=None,
        help="Optional regex filter for library files (default: None)"
    )

    parser.add_argument(
        "--metric",
        default="instruction_coverage_percent",
        help="Metric to plot (default: instruction_coverage_percent)"
    )

    parser.add_argument(
        "--save-path",
        required=True,
        help="Path to save the plot image"
    )

    parser.add_argument(
        "--show-rows",
        type=int,
        default=5,
        help="Number of rows to display from the dataframe (default: 5)"
    )

    parser.add_argument(
        "--smoothing-window",
        type=int,
        default=10,
        help="Smoothing window"
    )

    parser.add_argument(
        "--xmin",
        type=float,
        default=None,
        help="x-axis minimum"
    )

    parser.add_argument(
        "--xmax",
        type=float,
        default=None,
        help="x-axis maximum"
    )

    parser.add_argument(
        "--ymin",
        type=float,
        default=None,
        help="y-axis minimum"
    )

    parser.add_argument(
        "--ymax",
        type=float,
        default=None,
        help="y-axis maximum"
    )

    return parser


def create_plot_all_metrics_parser():
    """Create argument parser for plot_all_metrics mode."""
    parser = argparse.ArgumentParser(
        description="Plot all metrics in a grid for multiple report directories"
    )

    parser.add_argument("--mode", choices=["dump2csv", "csv2plot", "plot_coverage_comparison", "plot_all_metrics", "stats2plot"])

    parser.add_argument(
        "--report-dirs",
        nargs='+',
        required=True,
        help="List of report directory paths to process"
    )

    parser.add_argument(
        "--libs-regex-filter",
        default=None,
        help="Optional regex filter for library files (default: None)"
    )

    parser.add_argument(
        "--save-dir",
        required=True,
        help="Directory to save the plot images"
    )

    parser.add_argument(
        "--show-rows",
        type=int,
        default=5,
        help="Number of rows to display from the dataframe (default: 5)"
    )

    return parser

def create_plot_from_csv_parser():

    parser = argparse.ArgumentParser(
        description="Plot for already computed stats."
    )

    parser.add_argument("--mode", choices=["dump2csv", "csv2plot", "plot_coverage_comparison", "plot_all_metrics", "stats2plot"])

    parser.add_argument(
        "--stats-files",
        nargs="+",
        required=True,
        help="Path to precomputed stats"
    )

    parser.add_argument(
        "--save-path",
        required=True,
        help="Path to save the plot image"
    )

    parser.add_argument(
        "--show-bands",
        type=bool,
        default=True,
        help="Show bands"
    )

    parser.add_argument(
        "--metric",
        type=str,
        required=True,
        help="Metric to plot"
    )

    parser.add_argument(
        "--smoothing-window",
        type=int,
        default=10,
        help="Smoothing window"
    )

    parser.add_argument(
        "--xmin",
        type=float,
        default=None,
        help="x-axis minimum"
    )

    parser.add_argument(
        "--xmax",
        type=float,
        default=None,
        help="x-axis maximum"
    )

    parser.add_argument(
        "--ymin",
        type=float,
        default=None,
        help="y-axis minimum"
    )

    parser.add_argument(
        "--ymax",
        type=float,
        default=None,
        help="y-axis maximum"
    )

    parser.add_argument(
        "--labels",
        nargs="+",
        type=str,
        required=True,
        help="Labels in the same order as the stats files."
    )

    return parser

def plot_from_csv(args):
    df_stats = [pd.read_csv(stats_file) for stats_file in args.stats_files]

    plot_coverage_comparison(df_stats, metric=args.metric,
                                        show_bands=args.show_bands, save_path=args.save_path, smoothing_window=args.smoothing_window,
                                        xmin=args.xmin, xmax=args.xmax, ymin=args.ymin, ymax=args.ymax, labels=args.labels)

def main():
    """Main entry point for the script."""
    # First, parse only the mode argument
    mode_parser = argparse.ArgumentParser(
        description="Process JaCoCo coverage data: generate reports or analyze them",
        add_help=False
    )
    mode_parser.add_argument(
        "--mode",
        required=True,
        choices=["dump2csv", "csv2plot", "plot_coverage_comparison", "plot_all_metrics", "stats2plot"],
        help="Operation mode"
    )

    mode_args, remaining = mode_parser.parse_known_args()

    # Create the appropriate parser based on mode
    if mode_args.mode == "dump2csv":
        parser = create_dump2csv_parser()
        args = parser.parse_args()
        run_dump2csv(args)

    elif mode_args.mode == "csv2plot":
        parser = create_csv2plot_parser()
        args = parser.parse_args()
        run_csv2plot(args)

    elif mode_args.mode == "plot_coverage_comparison":
        parser = create_plot_coverage_comparison_parser()
        args = parser.parse_args()
        run_plot_coverage_comparison(args)

    elif mode_args.mode == "plot_all_metrics":
        parser = create_plot_all_metrics_parser()
        args = parser.parse_args()
        run_plot_all_metrics(args)

    elif mode_args.mode == "stats2plot":
        parser = create_plot_from_csv_parser()
        args = parser.parse_args()
        plot_from_csv(args)


if __name__ == "__main__":
    main()