#!/usr/bin/env python3

import argparse
import subprocess
import time
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
import shutil

class RustCoverageMonitor:
    def __init__(
        self,
        interval=3600,
        profraw_dir="cov/profiles",
        output_dir="coverage-dumps",
        llvm_profdata=None,
    ):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.interval = interval
        self.profraw_dir = Path(profraw_dir)
        self.output_dir = Path(output_dir) / f"dump-{timestamp}"
        self.llvm_profdata = llvm_profdata
        self.running = True
        self.dump_count = 0

        self.output_dir.mkdir(parents=True, exist_ok=True)

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _format_duration(self, seconds):
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            return f"{seconds//60}m {seconds%60}s"
        else:
            h = seconds // 3600
            m = (seconds % 3600) // 60
            s = seconds % 60
            return f"{h}h {m}m {s}s"

    def _timestamped_profdata_path(self):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.output_dir / f"coverage_{ts}.profdata"

    def _snapshot_profraw(self):
        """
        Copy current .profraw files to a temp dir so merging
        is stable even while the fuzzer keeps running.
        """
        snapshot_dir = self.output_dir / "profraw-snapshots" / datetime.now().strftime("%Y%m%d_%H%M%S")
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        profraw_files = list(self.profraw_dir.glob("*.profraw"))
        if not profraw_files:
            print("⚠️  No .profraw files found to snapshot")
            return None

        for f in profraw_files:
            shutil.copy2(f, snapshot_dir / f.name)

        return snapshot_dir

    def _merge_coverage(self):
        out_file = self._timestamped_profdata_path()

        # Gather list of profraw files before merging
        profraw_files = list(self.profraw_dir.glob("*.profraw"))
        if not profraw_files:
            print("⚠️  No .profraw files found to merge")
            return False

        print(f"📊 Merging {len(profraw_files)} profraw files → {out_file.name}")

        cmd = [
            self.llvm_profdata,
            "merge",
            "-sparse",
            *[str(f) for f in profraw_files],
            "-o",
            str(out_file),
        ]

        try:
            merge_start = time.perf_counter()
            subprocess.run(cmd, check=True)
            merge_elapsed = time.perf_counter() - merge_start

            if out_file.exists():
                size = out_file.stat().st_size
                self.dump_count += 1
                print(f"✅ Coverage snapshot created ({size:,} bytes) in {merge_elapsed:.2f}s")

                # Delete the profraw files that were merged
                deleted_count = 0
                for f in profraw_files:
                    try:
                        f.unlink()
                        deleted_count += 1
                    except OSError as e:
                        print(f"⚠️  Failed to delete {f.name}: {e}")
                print(f"🗑️  Deleted {deleted_count} profraw files")

                return True
            else:
                print("❌ profdata file was not created")
                return False

        except subprocess.CalledProcessError as e:
            print("❌ Failed to merge coverage")
            print(f"   Error: {e}")
            return False

    def start_monitoring(self):
        print("🚀 Starting Rust Coverage Monitor")
        print("=" * 60)
        print(f"📁 profraw dir: {self.profraw_dir}")
        print(f"📁 output dir:  {self.output_dir}")
        print(f"⏱️  interval:   {self._format_duration(self.interval)}")
        print(f"🛠 llvm-profdata: {self.llvm_profdata}")
        print("\n💡 Press Ctrl+C to stop\n")

        start_time = datetime.now()
        last_dump_time = None

        print("🎬 Taking initial snapshot...")
        if self._merge_coverage():
            last_dump_time = datetime.now()

        while self.running:
            try:
                if last_dump_time:
                    elapsed = (datetime.now() - last_dump_time).total_seconds()
                    remaining = max(0, self.interval - elapsed)
                else:
                    remaining = self.interval

                if remaining > 0:
                    sleep_interval = min(60, remaining)
                    if remaining <= 60:
                        print(f"⏳ Next snapshot in {int(remaining)}s...")
                    elif int(remaining) % 60 == 0:
                        print(f"⏳ Next snapshot in {self._format_duration(int(remaining))}...")
                    time.sleep(sleep_interval)
                    continue

                now = datetime.now()
                runtime = (now - start_time).total_seconds()
                print(f"\n⏰ {now.strftime('%Y-%m-%d %H:%M:%S')} (Runtime: {self._format_duration(int(runtime))})")

                if self._merge_coverage():
                    last_dump_time = datetime.now()
                    print(f"📈 Total snapshots: {self.dump_count}")

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"❌ Unexpected error: {e}")
                print("⏳ Retrying in 10s...")
                time.sleep(10)

        total_runtime = (datetime.now() - start_time).total_seconds()
        print("\n" + "=" * 60)
        print("📊 Coverage Monitoring Summary")
        print(f"   • Total runtime: {self._format_duration(int(total_runtime))}")
        print(f"   • Snapshots:     {self.dump_count}")
        print(f"   • Output dir:    {self.output_dir}")
        print("👋 Monitoring stopped")

def main():
    parser = argparse.ArgumentParser(
        description="Monitor LLVM (Rust) coverage by periodically snapshotting .profraw files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("-i", "--interval", type=int, default=3600,
                        help="Snapshot interval in seconds (default: 3600)")
    parser.add_argument("--profraw-dir", type=str, default="cov/profiles",
                        help="Directory where .profraw files are written")
    parser.add_argument("-o", "--output-dir", type=str, default="coverage-dumps",
                        help="Directory to store timestamped coverage snapshots")
    parser.add_argument("--llvm-profdata", type=str, required=True,
                        help="Path to llvm-profdata (Rust toolchain version)")

    args = parser.parse_args()

    if args.interval < 1:
        print("❌ Interval must be >= 1 second")
        sys.exit(1)

    if not Path(args.profraw_dir).exists():
        print(f"❌ profraw directory not found: {args.profraw_dir}")
        sys.exit(1)

    if not Path(args.llvm_profdata).exists():
        print(f"❌ llvm-profdata not found: {args.llvm_profdata}")
        sys.exit(1)

    monitor = RustCoverageMonitor(
        interval=args.interval,
        profraw_dir=args.profraw_dir,
        output_dir=args.output_dir,
        llvm_profdata=args.llvm_profdata,
    )

    monitor.start_monitoring()

if __name__ == "__main__":
    main()
