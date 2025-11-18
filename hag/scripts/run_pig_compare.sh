#!/usr/bin/env bash
set -euo pipefail
PIG_DIR="/opt/pig-scripts"

echo "=== Ejecutando comparación de WordCounts (Human vs LLM) ==="

pig -x mapreduce \
  -param in_h=hdfs://namenode:9000/output/wc_dataset \
  -param in_l=hdfs://namenode:9000/output/wc_llm \
  -param out=hdfs://namenode:9000/output/compare \
  -param N=50 \
  "$PIG_DIR/compare_topN.pig"

echo "Comparativa lista en HDFS:"
echo "  hdfs dfs -ls /output/compare"
echo "  hdfs dfs -cat /output/compare/top_diff/part-* | head"
