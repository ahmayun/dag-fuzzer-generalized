#!/bin/bash

echo "Checking for multi-version JARs..."
echo "=================================="

LIB_DIR=$1

for jar in $LIB_DIR/*.jar; do
    echo "Analyzing: $(basename "$jar")"

    # Check if META-INF/versions exists
    if jar tf "$jar" | grep -q "META-INF/versions/"; then
        echo "  ✓ Multi-version JAR detected"

        # List all version directories
        echo "  Available versions:"
        jar tf "$jar" | grep "META-INF/versions/" | grep -E "META-INF/versions/[0-9]+/$" | sed 's|META-INF/versions/||' | sed 's|/$||' | sort -n | while read version; do
            echo "    - Java $version"
        done

        # Show some example duplicate classes
        echo "  Sample versioned classes:"
        jar tf "$jar" | grep "META-INF/versions/" | grep "\.class$" | head -3 | while read class; do
            echo "    $class"
        done
    else
        echo "  - Standard JAR (no multi-version)"
    fi
    echo ""
done