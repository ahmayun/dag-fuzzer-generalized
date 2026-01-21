#!/usr/bin/env python3
"""
profdata-to-plot.py

Given a directory containing coverage_*.profdata snapshots, produce:
  - cumulative profdata files (each merged with all previous)
  - per-snapshot CSVs (file/package-level + totals)
  - a consolidated totals CSV (time series)
  - a coverage-vs-time plot (optionally interpolated)

Typical usage:
  python profdata-to-plot.py \
    --input-dir /path/to/dump-YYYYMMDD_HHMMSS \
    --output-dir example/ \
    --binary /path/to/_polars_runtime.abi3.so \
    --llvm-cov "$(rustc --print target-libdir)/../bin/llvm-cov" \
    --llvm-profdata "$(rustc --print target-libdir)/../bin/llvm-profdata"

Output layout:
  example/
    cumulative-profdata/
      cumulative_coverage_20260114_143300.profdata
      cumulative_coverage_20260114_153300.profdata
      ...
    snapshot-csvs/
      cumulative_coverage_20260114_143300.csv
      cumulative_coverage_20260114_153300.csv
      ...
    consolidated-csv/
      coverage_timeseries.csv
    plots/
      coverage_vs_time.png
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# -----------------------------
# Parsing + data structures
# -----------------------------

PROFDATA_TS_RE = re.compile(r"coverage_(\d{8})_(\d{6})\.profdata$")


@dataclass(frozen=True)
class CovRow:
    """A single row from llvm-cov report table."""
    name: str
    regions_cov_pct: float
    regions_covered: int
    regions_total: int
    functions_cov_pct: float
    functions_covered: int
    functions_total: int
    lines_cov_pct: float
    lines_covered: int
    lines_total: int


@dataclass(frozen=True)
class SnapshotResult:
    profdata_path: Path
    timestamp: datetime
    rows: List[CovRow]           # file-level rows (filtered)
    totals: CovRow               # TOTAL row


def parse_snapshot_timestamp(profdata_path: Path) -> datetime:
    # Handle both "coverage_*.profdata" and "cumulative_coverage_*.profdata"
    name = profdata_path.name
    if name.startswith("cumulative_"):
        name = name[len("cumulative_"):]

    m = PROFDATA_TS_RE.search(name)
    if not m:
        raise ValueError(
            f"Snapshot filename does not match expected pattern coverage_YYYYMMDD_HHMMSS.profdata: {profdata_path.name}"
        )
    ymd, hms = m.group(1), m.group(2)
    return datetime.strptime(ymd + hms, "%Y%m%d%H%M%S")


def _parse_int_pair(s: str) -> Tuple[int, int]:
    # Formats often look like "3715/210864"
    s = s.strip()
    if "/" not in s:
        raise ValueError(f"Expected covered/total format, got: {s}")
    a, b = s.split("/", 1)
    return int(a.strip().replace(",", "")), int(b.strip().replace(",", ""))


def _parse_pct(s: str) -> float:
    # Formats often look like "1.8%" or "0.4%"
    s = s.strip()
    if s.endswith("%"):
        s = s[:-1]
    return float(s)


def parse_llvm_cov_report_text(report_text: str) -> Tuple[List[CovRow], CovRow]:
    lines = [ln.rstrip("\n") for ln in report_text.splitlines() if ln.strip()]

    header_seen = False
    rows: List[CovRow] = []
    total: Optional[CovRow] = None

    for ln in lines:
        if ln.startswith("Filename"):
            header_seen = True
            continue
        if ln.startswith("---"):
            continue
        if not header_seen:
            continue

        # Split by whitespace, but keep filename intact by slicing
        # Filename column is fixed-width in llvm-cov output (~110 chars)
        name = ln[:110].strip()
        rest = ln[110:].split()

        if len(rest) < 12:
            continue  # malformed or unexpected

        regions_total = int(rest[0])
        regions_missed = int(rest[1])
        regions_pct = float(rest[2].rstrip("%"))
        regions_covered = regions_total - regions_missed

        functions_total = int(rest[3])
        functions_missed = int(rest[4])
        functions_pct = float(rest[5].rstrip("%"))
        functions_covered = functions_total - functions_missed

        lines_total = int(rest[6])
        lines_missed = int(rest[7])
        lines_pct = float(rest[8].rstrip("%"))
        lines_covered = lines_total - lines_missed

        row = CovRow(
            name=name,
            regions_cov_pct=regions_pct,
            regions_covered=regions_covered,
            regions_total=regions_total,
            functions_cov_pct=functions_pct,
            functions_covered=functions_covered,
            functions_total=functions_total,
            lines_cov_pct=lines_pct,
            lines_covered=lines_covered,
            lines_total=lines_total,
        )

        if name == "TOTAL":
            total = row
        else:
            rows.append(row)

    if total is None:
        raise ValueError("Parsed llvm-cov report but did not capture TOTAL row.")

    return rows, total



def group_key_for_path(path_str: str, group_regex: Optional[re.Pattern]) -> str:
    """
    Compute a "package-like" key from a file path.
    Default behavior (no regex): tries to extract the first polars-* directory component,
    else uses the parent directory name, else "unknown".
    """
    p = path_str.replace("\\", "/")

    if group_regex is not None:
        m = group_regex.search(p)
        if m:
            # If regex has a capture group, use it; otherwise use entire match
            return m.group(1) if m.lastindex else m.group(0)

    # Heuristic: prefer "polars-xxx" directory components
    comps = [c for c in p.split("/") if c]
    for c in comps:
        if c.startswith("polars-"):
            return c

    # fallback: parent directory
    if len(comps) >= 2:
        return comps[-2]

    return "unknown"


def aggregate_by_group(rows: List[CovRow], group_regex: Optional[re.Pattern]) -> Dict[str, CovRow]:
    """
    Aggregate coverage counts by group key. Percentages are recomputed from summed counts.
    """
    acc: Dict[str, Dict[str, int]] = {}

    for r in rows:
        g = group_key_for_path(r.name, group_regex)
        if g not in acc:
            acc[g] = {
                "regions_covered": 0, "regions_total": 0,
                "functions_covered": 0, "functions_total": 0,
                "lines_covered": 0, "lines_total": 0,
            }
        acc[g]["regions_covered"] += r.regions_covered
        acc[g]["regions_total"] += r.regions_total
        acc[g]["functions_covered"] += r.functions_covered
        acc[g]["functions_total"] += r.functions_total
        acc[g]["lines_covered"] += r.lines_covered
        acc[g]["lines_total"] += r.lines_total

    out: Dict[str, CovRow] = {}
    for g, d in acc.items():
        def pct(cov: int, total: int) -> float:
            return 0.0 if total == 0 else (100.0 * cov / total)

        out[g] = CovRow(
            name=g,
            regions_cov_pct=pct(d["regions_covered"], d["regions_total"]),
            regions_covered=d["regions_covered"],
            regions_total=d["regions_total"],
            functions_cov_pct=pct(d["functions_covered"], d["functions_total"]),
            functions_covered=d["functions_covered"],
            functions_total=d["functions_total"],
            lines_cov_pct=pct(d["lines_covered"], d["lines_total"]),
            lines_covered=d["lines_covered"],
            lines_total=d["lines_total"],
        )

    return out


# -----------------------------
# LLVM command runner
# -----------------------------

def run_llvm_cov_report(
    llvm_cov: str,
    binary: Path,
    profdata: Path,
    ignore_filename_regex: Optional[str],
) -> str:
    """
    Run: llvm-cov report <binary> --instr-profile=<profdata> [--ignore-filename-regex=...]
    Returns stdout text.
    """
    cmd = [
        llvm_cov,
        "report",
        str(binary),
        f"--instr-profile={profdata}",
    ]
    if ignore_filename_regex:
        cmd.append(f"--ignore-filename-regex={ignore_filename_regex}")

    try:
        res = subprocess.run(cmd, check=True, capture_output=True, text=True)
        return res.stdout
    except subprocess.CalledProcessError as e:
        msg = e.stderr.strip() if e.stderr else str(e)
        raise RuntimeError(f"llvm-cov report failed for {profdata.name}: {msg}") from e


def get_snapshot_result(
    llvm_cov: str,
    binary: Path,
    profdata: Path,
    ignore_filename_regex: Optional[str],
) -> SnapshotResult:
    ts = parse_snapshot_timestamp(profdata)
    text = run_llvm_cov_report(llvm_cov, binary, profdata, ignore_filename_regex)
    rows, totals = parse_llvm_cov_report_text(text)
    return SnapshotResult(profdata_path=profdata, timestamp=ts, rows=rows, totals=totals)


# -----------------------------
# CSV writers
# -----------------------------

def write_snapshot_csv(
    snapshot: SnapshotResult,
    out_csv: Path,
    group_regex: Optional[re.Pattern],
) -> None:
    """
    Writes a per-snapshot CSV with:
      - a TOTAL row
      - group/package aggregates
      - file-level rows (optional, but included for debugging)

    Columns are designed to be "jacoco-like": entity + covered/total + pct.
    """
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    group_agg = aggregate_by_group(snapshot.rows, group_regex)

    # We'll write three sections in one CSV with a "level" column:
    # level = total | group | file
    with out_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "timestamp",
            "level",
            "entity",
            "regions_pct", "regions_covered", "regions_total",
            "functions_pct", "functions_covered", "functions_total",
            "lines_pct", "lines_covered", "lines_total",
            "profdata_file",
        ])

        def emit(level: str, entity: str, r: CovRow):
            w.writerow([
                snapshot.timestamp.isoformat(timespec="seconds"),
                level,
                entity,
                f"{r.regions_cov_pct:.6f}", r.regions_covered, r.regions_total,
                f"{r.functions_cov_pct:.6f}", r.functions_covered, r.functions_total,
                f"{r.lines_cov_pct:.6f}", r.lines_covered, r.lines_total,
                snapshot.profdata_path.name,
            ])

        emit("total", "TOTAL", snapshot.totals)

        for g in sorted(group_agg.keys()):
            emit("group", g, group_agg[g])

        # file rows last (debug)
        for r in snapshot.rows:
            emit("file", r.name, r)


def write_consolidated_timeseries_csv(
    snapshots: List[SnapshotResult],
    out_csv: Path,
) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for s in sorted(snapshots, key=lambda x: x.timestamp):
        t = s.timestamp
        tot = s.totals
        rows.append({
            "timestamp": t.isoformat(timespec="seconds"),
            "epoch_seconds": int(t.timestamp()),
            "regions_pct": tot.regions_cov_pct,
            "regions_covered": tot.regions_covered,
            "regions_total": tot.regions_total,
            "functions_pct": tot.functions_cov_pct,
            "functions_covered": tot.functions_covered,
            "functions_total": tot.functions_total,
            "lines_pct": tot.lines_cov_pct,
            "lines_covered": tot.lines_covered,
            "lines_total": tot.lines_total,
            "profdata_file": s.profdata_path.name,
        })

    df = pd.DataFrame(rows).sort_values("epoch_seconds")
    df.to_csv(out_csv, index=False)


# -----------------------------
# Plotting
# -----------------------------

def plot_timeseries(
    consolidated_csv: Path,
    out_png: Path,
    interpolate: bool,
    interpolate_step_sec: float,
) -> None:
    """
    Plots raw coverage growth (covered counts) over elapsed time in seconds.
    """
    out_png.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(consolidated_csv)

    # Parse and sort timestamps
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    # Compute elapsed seconds since first snapshot
    t0 = df["timestamp"].iloc[0]
    df["elapsed_sec"] = (df["timestamp"] - t0).dt.total_seconds()

    plot_df = df[
        [
            "elapsed_sec",
            "regions_covered",
            "functions_covered",
            "lines_covered",
        ]
    ].copy()

    if interpolate and len(plot_df) >= 2:
        x_min = plot_df["elapsed_sec"].min()
        x_max = plot_df["elapsed_sec"].max()

        new_x = np.arange(x_min, x_max + interpolate_step_sec, interpolate_step_sec)

        # Set index and interpolate
        plot_df = plot_df.set_index("elapsed_sec")
        plot_df = plot_df.reindex(plot_df.index.union(new_x))
        plot_df = plot_df.sort_index()
        plot_df = plot_df.interpolate(method="linear")
        plot_df = plot_df.loc[new_x]
        plot_df = plot_df.reset_index()
        # Ensure the index column is named 'elapsed_sec'
        if 'index' in plot_df.columns and 'elapsed_sec' not in plot_df.columns:
            plot_df = plot_df.rename(columns={'index': 'elapsed_sec'})

    plt.figure(figsize=(8, 4))
    plt.plot(plot_df["elapsed_sec"], plot_df["regions_covered"], label="Regions covered")
    plt.plot(plot_df["elapsed_sec"], plot_df["functions_covered"], label="Functions covered")
    plt.plot(plot_df["elapsed_sec"], plot_df["lines_covered"], label="Lines covered")

    plt.xlabel("Time since start (seconds)")
    plt.ylabel("Covered count")
    plt.title("Cumulative Coverage Growth vs Time (LLVM / Rust)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()




# -----------------------------
# Main
# -----------------------------

def find_profdata_files(input_dir: Path) -> List[Path]:
    files = sorted(input_dir.glob("coverage_*.profdata"))
    # Validate naming pattern early
    valid = []
    for f in files:
        if PROFDATA_TS_RE.search(f.name):
            valid.append(f)
    return valid


def select_datapoints(
    sorted_inputs: List[Path],
    num_points: Optional[int],
) -> List[int]:
    """
    Select indices from sorted_inputs to create evenly-spaced data points.

    If num_points is None, compute default as 2 points per hour based on time span.
    Returns list of indices into sorted_inputs.
    """
    if len(sorted_inputs) == 0:
        return []

    if len(sorted_inputs) == 1:
        return [0]

    # Parse timestamps
    timestamps = [parse_snapshot_timestamp(p) for p in sorted_inputs]
    first_ts = timestamps[0]
    last_ts = timestamps[-1]
    time_span = (last_ts - first_ts).total_seconds()

    # Calculate default num_points if not specified (2 per hour)
    if num_points is None:
        hours = time_span / 3600.0
        num_points = max(2, int(hours * 2))  # At least 2 points

    # If we have fewer files than requested points, use all files
    if len(sorted_inputs) <= num_points:
        return list(range(len(sorted_inputs)))

    # Select evenly-spaced indices
    # Always include first (0) and last (len-1)
    indices = [0]

    if num_points > 2:
        # Distribute remaining points evenly in between
        step = (len(sorted_inputs) - 1) / (num_points - 1)
        for i in range(1, num_points - 1):
            idx = int(round(i * step))
            indices.append(idx)

    # Always include the last index
    if len(sorted_inputs) - 1 not in indices:
        indices.append(len(sorted_inputs) - 1)

    return sorted(set(indices))


def create_cumulative_profdata_files(
    input_profdatas: List[Path],
    output_dir: Path,
    llvm_profdata: str,
    num_datapoints: Optional[int],
) -> List[Path]:
    """
    Create cumulative profdata files where each snapshot is merged with all previous ones.

    Uses incremental merging for efficiency: each cumulative file is created by merging
    the previous cumulative file with new input files, rather than re-merging all files.

    For example, if input files are [t1, t2, ..., t100] and num_datapoints=3:
    - Select indices [0, 50, 99] (approximately)
    - cumulative_t1.profdata = merge(t1, ..., t10)
    - cumulative_t50.profdata = merge(cumulative_t1.profdata, t11, ..., t50)
    - cumulative_t100.profdata = merge(cumulative_t50.profdata, t51, ..., t100)

    Returns list of cumulative profdata files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    cumulative_files: List[Path] = []

    # Sort input files by timestamp to ensure correct ordering
    sorted_inputs = sorted(input_profdatas, key=lambda p: parse_snapshot_timestamp(p))

    # Select which data points to process
    selected_indices = select_datapoints(sorted_inputs, num_datapoints)

    print("\n🔄 Creating cumulative profdata files...")
    print("=" * 70)

    if num_datapoints is None:
        # Calculate what the default would be
        timestamps = [parse_snapshot_timestamp(p) for p in sorted_inputs]
        time_span_hours = (timestamps[-1] - timestamps[0]).total_seconds() / 3600.0
        default_points = max(2, int(time_span_hours * 2))
        print(f"📊 Time span: {time_span_hours:.2f} hours")
        print(f"📊 Selected {len(selected_indices)} data points (default: 2 per hour)")
    else:
        print(f"📊 Selected {len(selected_indices)} data points (requested: {num_datapoints})")

    print(f"📊 Total input files: {len(sorted_inputs)}")
    print()

    prev_cumulative: Optional[Path] = None
    prev_idx = -1

    for i, idx in enumerate(selected_indices, start=1):
        prof = sorted_inputs[idx]

        # Files to merge: new files since last cumulative point
        # If this is the first point, merge from the beginning
        start_idx = prev_idx + 1
        files_to_merge = sorted_inputs[start_idx:idx + 1]

        # Output filename maintains the timestamp of the latest file
        output_name = f"cumulative_{prof.name}"
        output_path = output_dir / output_name

        # Build llvm-profdata merge command
        cmd = [llvm_profdata, "merge", "-sparse"]

        # If we have a previous cumulative file, include it first
        if prev_cumulative is not None:
            cmd.append(str(prev_cumulative))

        # Add the new files
        cmd.extend([str(f) for f in files_to_merge])
        cmd.extend(["-o", str(output_path)])

        # Calculate total files represented
        total_files = idx + 1
        new_files = len(files_to_merge)

        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
            cumulative_files.append(output_path)

            if prev_cumulative is not None:
                print(f"✅ [{i}/{len(selected_indices)}] {output_name} "
                      f"(merged prev cumulative + {new_files} new file{'s' if new_files != 1 else ''}, "
                      f"total: {total_files} files)")
            else:
                print(f"✅ [{i}/{len(selected_indices)}] {output_name} "
                      f"(merged {new_files} file{'s' if new_files != 1 else ''})")

            # Update for next iteration
            prev_cumulative = output_path
            prev_idx = idx

        except subprocess.CalledProcessError as e:
            msg = e.stderr.strip() if e.stderr else str(e)
            raise RuntimeError(f"llvm-profdata merge failed for {output_name}: {msg}") from e

    print()
    return cumulative_files


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert coverage_*.profdata snapshots into cumulative coverage, per-snapshot CSVs, a consolidated CSV, and a coverage-vs-time plot."
    )

    parser.add_argument("--input-dir", required=True, type=str,
                        help="Directory containing coverage_*.profdata files (snapshots).")
    parser.add_argument("--output-dir", required=True, type=str,
                        help="Output directory root (creates cumulative-profdata/, snapshot-csvs/, consolidated-csv/, plots/).")

    parser.add_argument("--binary", required=True, type=str,
                        help="Path to the instrumented binary (e.g., _polars_runtime.abi3.so).")
    parser.add_argument("--llvm-cov", required=False, type=str, default=None,
                        help="Path to llvm-cov (should match Rust LLVM version). If omitted, uses 'llvm-cov' from PATH.")
    parser.add_argument("--llvm-profdata", required=False, type=str, default=None,
                        help="Path to llvm-profdata (should match Rust LLVM version). If omitted, uses 'llvm-profdata' from PATH.")

    parser.add_argument("--ignore-filename-regex", required=False, type=str,
                        default=r".cargo/registry|rustc|/usr/",
                        help="Regex passed to llvm-cov to ignore external sources (default filters registry/rustc/usr).")

    parser.add_argument("--group-regex", required=False, type=str, default=r"(polars-[^/]+)",
                        help=("Regex used to aggregate file paths into 'package' groups for per-snapshot CSVs. "
                              "If it has a capture group, group(1) is used. Default extracts polars-XXX components."))

    parser.add_argument("--no-interpolate", action="store_true",
                        help="Disable interpolation; plot only actual snapshot points connected by lines.")
    parser.add_argument("--interpolate-step-sec", type=float, default=30.0,
                        help="Interpolation step in seconds for elapsed-time axis (default: 30s).")

    parser.add_argument("--data-points", required=False, type=int, default=None,
                        help="Number of evenly-spaced data points to generate. Default: 2 points per hour based on time span.")


    args = parser.parse_args()

    input_dir = Path(args.input_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    binary = Path(args.binary).expanduser().resolve()
    llvm_cov = args.llvm_cov or "llvm-cov"
    llvm_profdata = args.llvm_profdata or "llvm-profdata"

    if not input_dir.exists():
        print(f"❌ input-dir not found: {input_dir}", file=sys.stderr)
        sys.exit(1)
    if not binary.exists():
        print(f"❌ binary not found: {binary}", file=sys.stderr)
        sys.exit(1)

    profdatas = find_profdata_files(input_dir)
    if not profdatas:
        print(f"❌ No coverage_*.profdata snapshots found in: {input_dir}", file=sys.stderr)
        sys.exit(1)

    # Compile group regex
    group_regex = re.compile(args.group_regex) if args.group_regex else None

    cumulative_dir = output_dir / "cumulative-profdata"
    snapshot_csv_dir = output_dir / "snapshot-csvs"
    consolidated_dir = output_dir / "consolidated-csv"
    plots_dir = output_dir / "plots"

    cumulative_dir.mkdir(parents=True, exist_ok=True)
    snapshot_csv_dir.mkdir(parents=True, exist_ok=True)
    consolidated_dir.mkdir(parents=True, exist_ok=True)
    plots_dir.mkdir(parents=True, exist_ok=True)

    print("🚀 Profdata → Cumulative Coverage → CSV/Plot")
    print("=" * 70)
    print(f"📥 input-dir:      {input_dir}")
    print(f"📦 binary:         {binary}")
    print(f"🛠 llvm-cov:        {llvm_cov}")
    print(f"🛠 llvm-profdata:   {llvm_profdata}")
    print(f"📁 output-dir:     {output_dir}")
    print(f"🧹 ignore regex:   {args.ignore_filename_regex}")
    print(f"🧩 group regex:    {args.group_regex}")

    # Step 1: Create cumulative profdata files
    cumulative_profdatas = create_cumulative_profdata_files(
        input_profdatas=profdatas,
        output_dir=cumulative_dir,
        llvm_profdata=llvm_profdata,
        num_datapoints=args.data_points,
    )

    # Step 2: Process cumulative snapshots
    snapshots: List[SnapshotResult] = []

    print("📊 Processing cumulative snapshots...")
    print("=" * 70)

    for i, prof in enumerate(cumulative_profdatas, start=1):
        try:
            snap = get_snapshot_result(
                llvm_cov=llvm_cov,
                binary=binary,
                profdata=prof,
                ignore_filename_regex=args.ignore_filename_regex,
            )
            snapshots.append(snap)

            out_csv = snapshot_csv_dir / (prof.stem + ".csv")
            write_snapshot_csv(snap, out_csv, group_regex)

            print(f"✅ [{i}/{len(cumulative_profdatas)}] {prof.name} → {out_csv.name} "
                  f"(lines {snap.totals.lines_cov_pct:.3f}%, funcs {snap.totals.functions_cov_pct:.3f}%, regions {snap.totals.regions_cov_pct:.3f}%)")

        except Exception as e:
            print(f"❌ Failed processing {prof.name}: {e}", file=sys.stderr)

    if not snapshots:
        print("❌ No snapshots processed successfully; cannot plot.", file=sys.stderr)
        sys.exit(1)

    consolidated_csv = consolidated_dir / "coverage_timeseries.csv"
    write_consolidated_timeseries_csv(snapshots, consolidated_csv)
    print(f"\n📌 Consolidated CSV: {consolidated_csv}")

    out_png = plots_dir / "coverage_vs_time.png"
    plot_timeseries(
        consolidated_csv=consolidated_csv,
        out_png=out_png,
        interpolate=(not args.no_interpolate),
        interpolate_step_sec=args.interpolate_step_sec,
    )

    print(f"📈 Plot saved:       {out_png}")
    print("\nDone. ✨")


if __name__ == "__main__":
    main()