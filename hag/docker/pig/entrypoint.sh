#!/usr/bin/env bash
set -euo pipefail

# Espera básica a HDFS (NameNode UI)
echo "Esperando a HDFS (NameNode) ..."
until curl -sf http://hadoop-namenode:9870 > /dev/null; do
  echo "  NameNode no listo; reintentando..."
  sleep 3
done

echo "Pig listo. Abrir una shell con: docker exec -it pig bash"
# Queda en sleep infinito; ejecutas scripts desde fuera o con docker exec
tail -f /dev/null
