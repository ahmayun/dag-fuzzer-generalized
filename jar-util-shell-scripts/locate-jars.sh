#!/bin/bash

JAR_DIR=$1
LIB_NAME=$2

for jar in $JAR_DIR/*.jar; do
  if jar tf "$jar" | grep -q $LIB_NAME; then
    echo "Found in: $jar"
  fi
done