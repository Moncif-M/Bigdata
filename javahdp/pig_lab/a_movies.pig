films = LOAD 'input/movies/films.json' USING JsonLoader('
    _id:chararray,
    title:chararray,
    year:int,
    genre:chararray,
    summary:chararray,
    country:chararray,
    director:(id:chararray),
    actors:{t:(id:chararray,role:chararray)}
');

artists = LOAD 'input/movies/artists.json' USING JsonLoader('
    _id:chararray,
    last_name:chararray,
    first_name:chararray,
    birth_date:chararray
');

films_us = FILTER films BY country == 'US';

group_year = GROUP films_us BY year;
films_by_year = FOREACH group_year GENERATE 
    group AS year, 
    COUNT(films_us) AS nb_films, 
    films_us.(title, genre) AS films;
sorted_year = ORDER films_by_year BY year;
STORE sorted_year INTO 'pigout/movies/us_by_year' USING PigStorage('|');

group_dir = GROUP films_us BY director.id;
films_by_dir = FOREACH group_dir GENERATE 
    group AS dir_id, 
    COUNT(films_us) AS nb_films, 
    films_us.(title, year) AS films;
STORE films_by_dir INTO 'pigout/movies/us_by_director' USING PigStorage('|');

films_act = FOREACH films_us GENERATE _id AS film_id, title AS film_title, actors AS actors_list;
flat_actors = FOREACH films_act GENERATE film_id, film_title, FLATTEN(actors_list) AS (actor_id:chararray, role:chararray);
triplets = FOREACH flat_actors GENERATE film_id, actor_id, role;
STORE triplets INTO 'pigout/movies/us_actors' USING PigStorage(',');

actors_for_join = FOREACH flat_actors GENERATE film_id, film_title, actor_id, role;
movies_actors = JOIN actors_for_join BY actor_id, artists BY _id;
movies_actors_fmt = FOREACH movies_actors GENERATE 
    actors_for_join::film_id AS film_id,
    actors_for_join::film_title AS film_title,
    artists::_id AS actor_id,
    artists::first_name AS first_name,
    artists::last_name AS last_name,
    artists::birth_date AS birth_date,
    actors_for_join::role AS role;
STORE movies_actors_fmt INTO 'pigout/movies/movies_actors' USING PigStorage(',');

films_cg = FOREACH films_us GENERATE _id AS film_id, title, year, genre, director.id AS dir_id;
full_cogroup = COGROUP films_cg BY film_id, movies_actors_fmt BY film_id;
full_movies = FOREACH full_cogroup GENERATE 
    group AS film_id,
    FLATTEN(films_cg.(title, year, genre)) AS (title, year, genre),
    movies_actors_fmt.(first_name, last_name, role) AS actors;
STORE full_movies INTO 'pigout/movies/full_movies' USING PigStorage('|');

films_dir = FOREACH films_us GENERATE director.id AS artist_id, _id AS film_id, title AS film_title;
dir_group = GROUP films_dir BY artist_id;
dir_list = FOREACH dir_group GENERATE group AS artist_id, films_dir.(film_id, film_title) AS films_directed;

acted = FOREACH flat_actors GENERATE actor_id AS artist_id, film_id, film_title, role;
act_group = GROUP acted BY artist_id;
act_list = FOREACH act_group GENERATE group AS artist_id, acted.(film_id, film_title, role) AS films_acted;

full_artists = COGROUP act_list BY artist_id FULL OUTER, dir_list BY artist_id;
artists_result = FOREACH full_artists GENERATE 
    group AS artist_id,
    (dir_list.films_directed IS NOT NULL ? dir_list.films_directed : TOBAG()) AS films_directed,
    (act_list.films_acted IS NOT NULL ? act_list.films_acted : TOBAG()) AS films_acted;
STORE artists_result INTO 'pigout/movies/artists_full' USING PigStorage('|');

