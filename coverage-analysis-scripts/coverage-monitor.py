#!/usr/bin/env python3

import argparse
import subprocess
import time
import os
import signal
import sys
from datetime import datetime
from pathlib import Path

class CoverageMonitor:
    def __init__(self, interval=3600, output_dir="coverage-dumps", host="localhost", port=6300, jacoco_path="jacoco-0.8.13/lib/jacococli.jar"):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.interval = interval
        self.output_dir = Path(f"{output_dir}/dump-{timestamp}")
        self.host = host
        self.port = port
        self.jacoco_path = jacoco_path
        self.running = True
        self.dump_count = 0

        # Create output directory
        self.output_dir.mkdir(exist_ok=True)

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _get_timestamp_filename(self):
        """Generate timestamped filename"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.output_dir / f"coverage-dump_{timestamp}.exec"

    def _dump_coverage(self):
        """Dump coverage from running JaCoCo agent"""
        filename = self._get_timestamp_filename()

        cmd = [
            'java', '-jar', self.jacoco_path, 'dump',
            '--address', self.host,
            '--port', str(self.port),
            '--destfile', str(filename)
        ]

        try:
            print(f"📊 Dumping coverage to {filename.name}...")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=30)

            if filename.exists():
                file_size = filename.stat().st_size
                self.dump_count += 1
                print(f"✅ Successfully dumped coverage ({file_size:,} bytes)")
                print(f"   📁 Saved to: {filename}")
                return True
            else:
                print(f"❌ Dump file was not created")
                return False

        except subprocess.TimeoutExpired:
            print(f"❌ Timeout while connecting to JaCoCo agent")
            return False
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to dump coverage:")
            print(f"   Error: {e.stderr.strip() if e.stderr else 'Unknown error'}")
            if "Connection refused" in str(e.stderr):
                print(f"   💡 Make sure your program is running with JaCoCo agent on {self.host}:{self.port}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False

    def _format_duration(self, seconds):
        """Format duration in human readable format"""
        if seconds < 60:
            return f"{seconds}s"
        elif seconds < 3600:
            return f"{seconds//60}m {seconds%60}s"
        else:
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            secs = seconds % 60
            return f"{hours}h {minutes}m {secs}s"

    def start_monitoring(self):
        """Start the coverage monitoring loop"""
        print("🚀 Starting Coverage Monitor")
        print("=" * 50)
        print(f"📍 Target: {self.host}:{self.port}")
        print(f"⏱️  Interval: {self._format_duration(self.interval)}")
        print(f"📁 Output Directory: {self.output_dir}")
        print(f"🔧 JaCoCo CLI: {self.jacoco_path}")
        print("\n💡 Press Ctrl+C to stop monitoring\n")

        start_time = datetime.now()
        last_dump_time = None

        # Initial dump
        print(f"🎬 Taking initial coverage dump...")
        success = self._dump_coverage()
        if success:
            last_dump_time = datetime.now()

        # Main monitoring loop
        while self.running:
            try:
                # Calculate time until next dump
                if last_dump_time:
                    elapsed = (datetime.now() - last_dump_time).total_seconds()
                    remaining = max(0, self.interval - elapsed)
                else:
                    remaining = self.interval

                if remaining > 0:
                    # Show countdown every 60 seconds or when less than 60 seconds remain
                    sleep_interval = min(60, remaining)
                    if remaining <= 60:
                        print(f"⏳ Next dump in {int(remaining)}s...")
                    elif remaining > 60 and int(remaining) % 60 == 0:
                        print(f"⏳ Next dump in {self._format_duration(int(remaining))}...")

                    time.sleep(sleep_interval)
                    continue

                # Time for next dump
                current_time = datetime.now()
                runtime = current_time - start_time

                print(f"\n⏰ {current_time.strftime('%Y-%m-%d %H:%M:%S')} (Runtime: {self._format_duration(int(runtime.total_seconds()))})")

                success = self._dump_coverage()
                if success:
                    last_dump_time = datetime.now()
                    print(f"📈 Total dumps collected: {self.dump_count}")

            except KeyboardInterrupt:
                print(f"\n🛑 Interrupted by user")
                break
            except Exception as e:
                print(f"❌ Unexpected error in monitoring loop: {e}")
                print(f"⏳ Continuing in 10 seconds...")
                time.sleep(10)

        # Final summary
        total_runtime = datetime.now() - start_time
        print(f"\n" + "=" * 50)
        print(f"📊 Coverage Monitoring Summary")
        print(f"   • Total runtime: {self._format_duration(int(total_runtime.total_seconds()))}")
        print(f"   • Total dumps collected: {self.dump_count}")
        print(f"   • Files saved in: {self.output_dir}")

        if self.dump_count > 0:
            print(f"\n💡 To analyze the collected coverage data, run:")
            print(f"   python3 analyze_coverage.py")

        print(f"👋 Coverage monitoring stopped.")

def main():
    parser = argparse.ArgumentParser(
        description="Monitor JaCoCo coverage from a running program at regular intervals",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dump coverage every hour (3600 seconds)
  python3 coverage-monitor.py --interval 3600

  # Dump coverage every 30 minutes
  python3 coverage-monitor.py --interval 1800

  # Dump coverage every 5 minutes with custom output directory
  python3 coverage-monitor.py --interval 300 --output-dir my-coverage-data

  # Monitor different host/port
  python3 coverage-monitor.py --host 192.168.1.100 --port 6301
        """
    )

    parser.add_argument(
        '--interval', '-i',
        type=int,
        default=3600,
        help='Dump interval in seconds (default: 3600 = 1 hour)'
    )

    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default='coverage-dumps',
        help='Output directory for coverage dumps (default: coverage-dumps)'
    )

    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='JaCoCo agent host (default: localhost)'
    )

    parser.add_argument(
        '--port', '-p',
        type=int,
        default=6300,
        help='JaCoCo agent port (default: 6300)'
    )

    parser.add_argument(
        '--jacoco-path',
        type=str,
        default='jacoco-0.8.13/lib/jacococli.jar',
        help='Path to JaCoCo CLI jar file (default: jacoco-0.8.13/lib/jacococli.jar)'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.interval < 1:
        print("❌ Error: Interval must be at least 1 second")
        sys.exit(1)

    if not Path(args.jacoco_path).exists():
        print(f"❌ Error: JaCoCo CLI not found at {args.jacoco_path}")
        sys.exit(1)

    # Create and start monitor
    monitor = CoverageMonitor(
        interval=args.interval,
        output_dir=args.output_dir,
        host=args.host,
        port=args.port,
        jacoco_path=args.jacoco_path
    )

    try:
        monitor.start_monitoring()
    except KeyboardInterrupt:
        print(f"\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()