#!/usr/bin/env bash
set -euo pipefail

echo "=== Cargando dataset y stopwords en HDFS ==="

hdfs dfs -mkdir -p /input
hdfs dfs -mkdir -p /output

hdfs dfs -put -f /data/dataset.csv /input/dataset.csv
hdfs dfs -put -f /pig/stopwords_es.txt /input/stopwords_es.txt

echo "Archivos cargados en HDFS:"
hdfs dfs -ls -h /input
