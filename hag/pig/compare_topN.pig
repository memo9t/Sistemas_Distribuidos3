%default in_h 'hdfs://namenode:9000/output/wc_dataset';
%default in_l 'hdfs://namenode:9000/output/wc_llm';
%default out  'hdfs://namenode:9000/output/compare';
%default N '50';

-- Cargar outputs previos de wordcount (token \t cnt)
H = LOAD '$in_h' USING PigStorage('\t') AS (token:chararray, cnt_h:long);
L = LOAD '$in_l' USING PigStorage('\t') AS (token:chararray, cnt_l:long);

-- TopN por separado
H_sorted = ORDER H BY cnt_h DESC;
H_topN   = LIMIT H_sorted (int)$N;

L_sorted = ORDER L BY cnt_l DESC;
L_topN   = LIMIT L_sorted (int)$N;

-- Full outer para comparar frecuencias globales
J = JOIN H BY token FULL OUTER, L BY token;

-- Relleno nulos con 0
J2 = FOREACH J GENERATE
       (H::token IS NOT NULL ? H::token : L::token) AS token:chararray,
       (H::cnt_h IS NOT NULL ? H::cnt_h : 0L) AS cnt_h:long,
       (L::cnt_l IS NOT NULL ? L::cnt_l : 0L) AS cnt_l:long,
       ((H::cnt_h IS NOT NULL ? H::cnt_h : 0L) - (L::cnt_l IS NOT NULL ? L::cnt_l : 0L)) AS delta:long;

-- Palabras exclusivas de humanos
ONLY_H = FILTER J2 BY (cnt_h > 0L) AND (cnt_l == 0L);
ONLY_H = ORDER ONLY_H BY cnt_h DESC;

-- Palabras exclusivas de LLM
ONLY_L = FILTER J2 BY (cnt_l > 0L) AND (cnt_h == 0L);
ONLY_L = ORDER ONLY_L BY cnt_l DESC;

-- Mayores diferencias absolutas
ABS = FOREACH J2 GENERATE token, cnt_h, cnt_l,
        (delta>=0?delta:-delta) AS adelta:long, delta;
ABS = ORDER ABS BY adelta DESC;

STORE H_topN INTO '$out/topN_human' USING PigStorage('\t');
STORE L_topN INTO '$out/topN_llm'   USING PigStorage('\t');
STORE ONLY_H  INTO '$out/only_human' USING PigStorage('\t');
STORE ONLY_L  INTO '$out/only_llm'   USING PigStorage('\t');
STORE ABS     INTO '$out/top_diff'   USING PigStorage('\t');
