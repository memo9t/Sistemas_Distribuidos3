# Análisis Distribuido de Respuestas Humanas vs LLM usando Hadoop + Pig

Este proyecto implementa un pipeline distribuido de análisis lingüístico utilizando:
- Hadoop 3.3.6 (HDFS)
- Apache Pig 0.17 en modo MapReduce
- Docker + Docker Compose

El sistema procesa un dataset con preguntas y respuestas, compara:
- dataset_answer (respuesta humana)
- llm_answer (respuesta generada por un LLM)

Y genera:
- WordCount por tipo de respuesta
- Tokens exclusivos humanos y exclusivos LLM
- Top-N más frecuentes
- Palabras con mayor diferencia absoluta de frecuencia
  
# Instalación y uso
- Clonar el repositorio
```bash
git clone <link del github>
cd <carpeta raiz del repositorio>
```

# Levantar el sistema
```bash
docker compose up -d
```
- Verificar que los contenedores estén corriendo

```bash
docker  ps

```
dejar unos 30 segundos corriendo y :
- Verificar estado del HDFS

```bash
docker exec -it namenode hdfs dfsadmin -report

```
# Cargar el dataset y stopwords al contenedor
Copia ambos archivos al NameNode:
```bash
docker cp data/input/dataset.csv  namenode:/data/dataset.csv
docker cp pig/stopwords_es.txt    namenode:/data/stopwords_es.txt

```
- Crea carpetas en HDFS:
```bash
docker exec -it namenode hdfs dfs -mkdir -p /input
docker exec -it namenode hdfs dfs -mkdir -p /output

```
- Sube los archivos:
```bash
docker exec -it namenode hdfs dfs -put -f /data/dataset.csv /input/
docker exec -it namenode hdfs dfs -put -f /data/stopwords_es.txt /input/

```
- Confirmar:
```bash
docker exec -it namenode hdfs dfs -ls /input

```
# Ejecutar WordCount con Pig
- Copiar scripts al contenedor Pig: 
```bash
docker cp scripts/run_pig_wordcount.sh pig:/opt/run_pig_wordcount.sh
docker cp pig/.                       pig:/opt/pig-scripts/
docker exec -it pig chmod +x /opt/run_pig_wordcount.sh

```
- Ejecutar:
```bash
docker exec -it pig /opt/run_pig_wordcount.sh

```
- Ver resultados:
```bash
docker exec -it namenode hdfs dfs -ls /output/wc_dataset
docker exec -it namenode hdfs dfs -ls /output/wc_llm

```
# Ejecutar comparador humano vs LLM
- Copiar script:
```bash
docker cp scripts/run_pig_compare.sh pig:/opt/run_pig_compare.sh
docker exec -it pig chmod +x /opt/run_pig_compare.sh

```
- Ejecutar:
```bash
docker exec -it pig /opt/run_pig_compare.sh

```
- Ver carpeta generada:
```bash
docker exec -it namenode hdfs dfs -ls /output/compare

```
# Ver resultados
muestra de resultados:
```bash
docker exec -it namenode hdfs dfs -cat /output/compare/<poner nombre de alguna carpeta generada anteriormente>/part*

```

# Apagar el sistema
```bash
docker compose down -v
```





























