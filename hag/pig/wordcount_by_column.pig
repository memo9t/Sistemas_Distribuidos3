-- ============================================================
-- Parámetros con default (solo para debugging local)
-- ============================================================
%default input 'hdfs:///input/dataset.csv';
%default column 'dataset_answer';
%default stopwords 'hdfs:///input/stopwords_es.txt';
%default out 'hdfs:///output/wc';

-- ============================================================
-- Cargar dataset desde HDFS
-- ============================================================
raw = LOAD '$input' USING PigStorage(',') AS
      (question_id:chararray,
       question:chararray,
       dataset_answer:chararray,
       llm_answer:chararray,
       score:chararray,
       ts_ms:chararray,
       decided_by:chararray);

-- Quitar encabezado
nohdr = FILTER raw BY question_id != 'question_id';

-- Seleccionar columna dinámica
proj = FOREACH nohdr GENERATE
    (CASE
        WHEN '$column' == 'dataset_answer' THEN dataset_answer
        WHEN '$column' == 'llm_answer'     THEN llm_answer
        ELSE dataset_answer
    END) AS text:chararray;

-- ============================================================
-- Limpieza INLINE (puede reemplazarse por normalize_clean)
-- ============================================================
cleaned = FOREACH proj GENERATE
            TRIM(
                REPLACE(
                    REPLACE(LOWER(text), '[^a-záéíóúüñ0-9 ]', ' '),
                    '\\s+',
                    ' '
                )
            ) AS text;

-- Tokenización
tokens = FOREACH cleaned GENERATE FLATTEN(TOKENIZE(text)) AS token;

-- ============================================================
-- Stopwords desde HDFS (OJO: debe estar en hdfs:///input/)
-- ============================================================
sw = LOAD '$stopwords' USING PigStorage('\n') AS (w:chararray);

j = JOIN tokens BY token LEFT OUTER, sw BY w;

no_sw = FILTER j BY w IS NULL;

only_tok = FOREACH no_sw GENERATE tokens::token AS token;

-- Conteo
grp = GROUP only_tok BY token;

wc = FOREACH grp GENERATE group AS token, COUNT(only_tok) AS cnt;

sorted = ORDER wc BY cnt DESC;

-- ============================================================
-- Output al HDFS
-- ============================================================
STORE sorted INTO '$out' USING PigStorage('\t');
