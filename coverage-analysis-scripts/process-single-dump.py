if __name__ == "__main__":
#     Option 1: Process from dump files (generates reports)
    df = process_jacoco_dumps(
        "jacoco-0.8.13/lib/jacococli.jar",
        "lib-spark-only",
        None,
        "cluster-results/dagfuzz/flink-python/headnode/dump-20250927_123145",
        ".test_reports"
    )