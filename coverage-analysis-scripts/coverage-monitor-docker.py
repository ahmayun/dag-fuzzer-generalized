#!/usr/bin/env python3
"""
Periodic JaCoCo dumps when the agent runs inside Docker (e.g. Spark Thrift + -javaagent).

Host-side `jacococli dump --address 127.0.0.1:6300` often times out against a published
container port even though TCP connect succeeds. This script runs `jacococli` in a
short-lived container that shares the target container's network namespace, so it
connects to 127.0.0.1:<port> inside that namespace (same as a working in-container dump).

For JVMs running directly on the host, use coverage-monitor.py instead.
"""

import argparse
import subprocess
import time
import signal
import sys
from datetime import datetime
from pathlib import Path


class CoverageMonitorDocker:
    def __init__(
        self,
        interval=3600,
        output_dir="coverage-dumps",
        port=6300,
        jacoco_path="jacoco-0.8.13/lib/jacococli.jar",
        network_container=None,
        docker_client_image="apache/spark:3.5.1",
        use_sudo_docker=False,
    ):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.interval = interval
        self.output_dir = Path(f"{output_dir}/dump-{timestamp}")
        self.port = port
        self.jacoco_path = jacoco_path
        self.network_container = network_container
        self.docker_client_image = docker_client_image
        self.use_sudo_docker = use_sudo_docker
        self.running = True
        self.dump_count = 0

        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Client image (e.g. apache/spark) runs as non-root uid 185; bind-mounted dump dir
        # must be writable by that uid or jacococli gets Permission denied on /out/...
        try:
            self.output_dir.chmod(0o1777)
        except OSError as e:
            print(f"⚠️  Warning: could not chmod {self.output_dir} to 1777: {e}")

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        self.running = False

    def _get_timestamp_filename(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.output_dir / f"coverage-dump_{timestamp}.exec"

    def _docker_prefix(self):
        return (["sudo"] if self.use_sudo_docker else []) + ["docker"]

    def _dump_coverage(self):
        filename = self._get_timestamp_filename()
        jacoco_cli = str(Path(self.jacoco_path).resolve())
        out_dir = str(self.output_dir.resolve())

        cmd = self._docker_prefix() + [
            "run",
            "--rm",
            "--network",
            f"container:{self.network_container}",
            "-v",
            f"{jacoco_cli}:/jacococli.jar:ro",
            "-v",
            f"{out_dir}:/out",
            self.docker_client_image,
            "java",
            "-jar",
            "/jacococli.jar",
            "dump",
            "--address",
            "127.0.0.1",
            "--port",
            str(self.port),
            "--destfile",
            f"/out/{filename.name}",
        ]

        try:
            print(f"📊 Dumping coverage to {filename.name}...")
            result = subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=120)

            if filename.exists():
                file_size = filename.stat().st_size
                self.dump_count += 1
                print(f"✅ Successfully dumped coverage ({file_size:,} bytes)")
                print(f"   📁 Saved to: {filename}")
                return True
            print(f"❌ Dump file was not created")
            if result.stdout:
                print(result.stdout.strip())
            return False

        except subprocess.TimeoutExpired:
            print(f"❌ Timeout while running jacococli inside Docker")
            print(
                f"   💡 Check container {self.network_container!r} is running and "
                f"JaCoCo tcpserver listens on port {self.port} in that network namespace."
            )
            return False
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to dump coverage:")
            err = (e.stderr or "").strip()
            out = (e.stdout or "").strip()
            if err:
                print(f"   stderr: {err}")
            if out:
                print(f"   stdout: {out}")
            return False
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            return False

    def _format_duration(self, seconds):
        if seconds < 60:
            return f"{seconds}s"
        if seconds < 3600:
            return f"{seconds//60}m {seconds%60}s"
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours}h {minutes}m {secs}s"

    def start_monitoring(self):
        print("🚀 Starting Coverage Monitor (Docker network namespace)")
        print("=" * 50)
        print(f"📍 Target container: {self.network_container!r}")
        print(f"   Connect inside namespace: 127.0.0.1:{self.port}")
        print(f"⏱️  Interval: {self._format_duration(self.interval)}")
        print(f"📁 Output Directory: {self.output_dir}")
        print(f"🔧 JaCoCo CLI: {self.jacoco_path}")
        print(f"🐳 Client image: {self.docker_client_image}")
        if self.use_sudo_docker:
            print("🐳 Docker: using sudo")
        print("\n💡 Press Ctrl+C to stop monitoring\n")

        start_time = datetime.now()
        last_dump_time = None

        print(f"🎬 Taking initial coverage dump...")
        success = self._dump_coverage()
        if success:
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
                        print(f"⏳ Next dump in {int(remaining)}s...")
                    elif remaining > 60 and int(remaining) % 60 == 0:
                        print(f"⏳ Next dump in {self._format_duration(int(remaining))}...")

                    time.sleep(sleep_interval)
                    continue

                current_time = datetime.now()
                runtime = current_time - start_time
                print(
                    f"\n⏰ {current_time.strftime('%Y-%m-%d %H:%M:%S')} "
                    f"(Runtime: {self._format_duration(int(runtime.total_seconds()))})"
                )

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
        description=(
            "Periodic JaCoCo dumps when the agent listens inside a Docker container. "
            "Uses `docker run --network container:NAME` so jacococli reaches 127.0.0.1:PORT "
            "in that namespace (avoids host-side dump hangs against published ports)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Spark Thrift in container `spark-thrift`, JaCoCo on 6300, dump every 5 minutes
  python3 coverage-monitor-docker.py --container spark-thrift --port 6300 --interval 300 \\
    --output-dir coverage-dumps/sqlancer-spark --sudo-docker

  # Same without sudo (user in docker group)
  python3 coverage-monitor-docker.py --container spark-thrift -p 6300 -i 300 \\
    -o coverage-dumps/sqlancer-spark
        """,
    )

    parser.add_argument(
        "--container",
        "-c",
        required=True,
        metavar="NAME",
        help="Running Docker container name (or id) whose network namespace hosts JaCoCo",
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=int,
        default=3600,
        help="Dump interval in seconds (default: 3600)",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default="coverage-dumps",
        help="Base output directory (default: coverage-dumps); creates dump-<timestamp>/ inside",
    )
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=6300,
        help="JaCoCo agent TCP port inside the target container (default: 6300)",
    )
    parser.add_argument(
        "--jacoco-path",
        type=str,
        default="jacoco-0.8.13/lib/jacococli.jar",
        help="Path to jacococli.jar on the host (default: jacoco-0.8.13/lib/jacococli.jar)",
    )
    parser.add_argument(
        "--docker-client-image",
        type=str,
        default="apache/spark:3.5.1",
        help="Image for the short jacococli client container (default: apache/spark:3.5.1)",
    )
    parser.add_argument(
        "--sudo-docker",
        action="store_true",
        help="Run docker via sudo",
    )

    args = parser.parse_args()

    if args.interval < 1:
        print("❌ Error: Interval must be at least 1 second")
        sys.exit(1)

    if not Path(args.jacoco_path).exists():
        print(f"❌ Error: JaCoCo CLI not found at {args.jacoco_path}")
        sys.exit(1)

    probe = (["sudo"] if args.sudo_docker else []) + ["docker", "inspect", args.container]
    try:
        subprocess.run(probe, capture_output=True, text=True, check=True, timeout=10)
    except FileNotFoundError:
        print("❌ Error: docker not found in PATH")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error: cannot inspect container {args.container!r}")
        if e.stderr:
            print(e.stderr.strip())
        sys.exit(1)
    except subprocess.TimeoutExpired:
        print("❌ Error: docker inspect timed out")
        sys.exit(1)

    monitor = CoverageMonitorDocker(
        interval=args.interval,
        output_dir=args.output_dir,
        port=args.port,
        jacoco_path=args.jacoco_path,
        network_container=args.container,
        docker_client_image=args.docker_client_image,
        use_sudo_docker=args.sudo_docker,
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
