#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: $0 <lib_directory> <java_version> <output_directory>"
    echo "Example: $0 lib-flink-filtered 11 lib-flink-cleaned"
    echo "Note: java_version is ignored - all META-INF/versions will be removed to avoid conflicts"
    exit 1
fi

LIB_DIR="$(realpath "$1")"
TARGET_VERSION="$2"
OUT_DIR="$(realpath "$3")"
ORIGINAL_DIR="$(pwd)"

if [ ! -d "$LIB_DIR" ]; then
    echo "Error: Directory $LIB_DIR does not exist"
    exit 1
fi

# Create output directory
mkdir -p "$OUT_DIR"

echo "Cleaning multi-version JARs from $LIB_DIR"
echo "Output directory: $OUT_DIR"
echo "Strategy: Removing all META-INF/versions to avoid class conflicts"
echo "================================================================="

for jar_file in "$LIB_DIR"/*.jar; do
    if [ ! -f "$jar_file" ]; then
        continue
    fi

    jar_name=$(basename "$jar_file")
    output_jar="$OUT_DIR/$jar_name"
    echo "Processing: $jar_name"

    # Check if it's a multi-version JAR
    if ! jar tf "$jar_file" | grep -q "META-INF/versions/"; then
        echo "  - Standard JAR (no multi-version), copying as-is"
        cp "$jar_file" "$output_jar"
        continue
    fi

    echo "  ✓ Multi-version JAR found - removing all META-INF/versions"

    # Create temporary directory for extraction
    temp_dir=$(mktemp -d)

    # Extract the JAR using absolute path
    cd "$temp_dir"
    jar xf "$jar_file"

    # Remove the entire META-INF/versions directory
    if [ -d "META-INF/versions" ]; then
        echo "    Removing entire META-INF/versions directory"
        rm -rf META-INF/versions
    fi

    # Create cleaned JAR in output directory using absolute path
    jar cf "$output_jar" .

    # Return to original directory and cleanup
    cd "$ORIGINAL_DIR"
    rm -rf "$temp_dir"

    echo "  ✅ Created cleaned $jar_name in $OUT_DIR"
    echo ""
done

echo "Done! Cleaned JARs are in: $OUT_DIR"
echo "Original JARs in $LIB_DIR remain untouched."
echo "All multi-version structures removed to prevent JaCoCo conflicts."
echo "You can now try running JaCoCo with: --classfiles $OUT_DIR/"