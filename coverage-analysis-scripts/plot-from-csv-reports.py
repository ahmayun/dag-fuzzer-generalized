import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process all JaCoCo reports and generate coverage dataframe"
    )

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

    args = parser.parse_args()

    df = process_all_reports(
        args.reports_dir,
        libs_regex_filter=args.libs_regex_filter
    )

    print(df.head(args.show_rows))