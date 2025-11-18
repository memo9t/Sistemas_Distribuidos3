#!/usr/bin/env bash
set -euo pipefail

echo "=== Descargando resultados desde HDFS ==="

mkdir -p ./data/output/hdfs_out


hdfs dfs -get -f /output/* ./data/output/hdfs_out/

echo "Resultados descargados en ./data/output/hdfs_out:"
find ./data/output/hdfs_out -maxdepth 3 -type f -print
