txt_lines = LOAD '/shared_volume/alice.txt' AS (line:chararray);

txt_words = FOREACH txt_lines GENERATE FLATTEN(TOKENIZE(line)) AS word;

words_clean = FILTER txt_words BY word MATCHES '\\w+';

grp_words = GROUP words_clean BY word;

cnt_words = FOREACH grp_words GENERATE 
    group AS word, 
    COUNT(words_clean) AS count;

cnt_words_sorted = ORDER cnt_words BY count DESC;

STORE cnt_words_sorted INTO '/shared_volume/pig_out/WORD_COUNT/';

top20_words = LIMIT cnt_words_sorted 20;
DUMP top20_words;

