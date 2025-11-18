#!/usr/bin/env bash
set -euo pipefail
PIG_DIR="/opt/pig-scripts"

echo "=== Ejecutando WordCount con Pig ==="

pig -x mapreduce \
  -param input=hdfs://namenode:9000/input/dataset.csv \
  -param column=dataset_answer \
  -param stopwords=hdfs://namenode:9000/input/stopwords_es.txt \
  -param out=hdfs://namenode:9000/output/wc_dataset \
  "$PIG_DIR/wordcount_by_column.pig"

pig -x mapreduce \
  -param input=hdfs://namenode:9000/input/dataset.csv \
  -param column=llm_answer \
  -param stopwords=hdfs://namenode:9000/input/stopwords_es.txt \
  -param out=hdfs://namenode:9000/output/wc_llm \
  "$PIG_DIR/wordcount_by_column.pig"

echo "WordCount completado"
