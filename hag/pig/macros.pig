-- ============================================================
-- Macro: normalize_clean(text)
-- Normaliza y limpia texto
-- ============================================================

DEFINE normalize_clean(text) RETURNS out {
    lc       = LOWER($text);
    stripped = REPLACE(lc, '[^a-záéíóúüñ0-9 ]', ' ');
    norm     = REPLACE(stripped, '\\s+', ' ');
    $out     = TRIM(norm);
};

-- ============================================================
-- Macro: explode_tokens(text)
-- Retorna bag de tokens
-- ============================================================

DEFINE explode_tokens(text) RETURNS tokens {
    split = TOKENIZE($text);
    fl    = FOREACH split GENERATE FLATTEN($0) AS token;
    $tokens = FILTER fl BY token IS NOT NULL AND SIZE(token) > 0;
};

-- ============================================================
-- Macro: filter_stopwords(tokens, sw)
-- Filtra stopwords (ambos bags)
-- ============================================================

DEFINE filter_stopwords(tokens, sw) RETURNS out {
    j = JOIN $tokens BY token LEFT OUTER, $sw BY w;
    f = FILTER j BY w IS NULL;
    $out = FOREACH f GENERATE $tokens::token AS token;
};
