films = LOAD 'input/movies/films.json' 
    USING JsonLoader('
        _id:chararray,
        title:chararray,
        year:int,
        genre:chararray,
        summary:chararray,
        country:chararray,
        director:(id:chararray),
        actors:{t:(id:chararray,role:chararray)}
    ');

films_us = FILTER films BY country == 'US';

grp_year = GROUP films_us BY year;

res_year = FOREACH grp_year GENERATE 
    group AS year,
    COUNT(films_us) AS nb_films,
    films_us.(title, genre) AS films;

sorted_year = ORDER res_year BY year;

DUMP sorted_year;
STORE sorted_year INTO 'pigout/movies/mUSA_annee' USING PigStorage('|');
-----------------------------------------------------------------------------------------------
films = LOAD 'input/movies/films.json' 
    USING JsonLoader('
        _id:chararray,
        title:chararray,
        year:int,
        genre:chararray,
        summary:chararray,
        country:chararray,
        director:(id:chararray),
        actors:{t:(id:chararray,role:chararray)}
    ');

films_us = FILTER films BY country == 'US';

grp_director = GROUP films_us BY director.id;

res_director = FOREACH grp_director GENERATE 
    group AS director_id,
    COUNT(films_us) AS nb_films,
    films_us.(title, year) AS films;

sorted_director = ORDER res_director BY nb_films DESC;

DUMP sorted_director;
STORE sorted_director INTO 'pigout/movies/mUSA_director' USING PigStorage('|');
------------------------------------------------------------------------------------------

films = LOAD 'input/movies/films.json' 
    USING JsonLoader('
        _id:chararray,
        title:chararray,
        year:int,
        genre:chararray,
        summary:chararray,
        country:chararray,
        director:(id:chararray),
        actors:{t:(id:chararray,role:chararray)}
    ');

films_us = FILTER films BY country == 'US';

films_actors = FOREACH films_us GENERATE 
    _id AS film_id,
    title AS film_title,
    actors AS actors_list;

flattened_actors = FOREACH films_actors GENERATE 
    film_id,
    film_title,
    FLATTEN(actors_list) AS (actor_id:chararray, role:chararray);

triplets = FOREACH flattened_actors GENERATE 
    film_id,
    actor_id,
    role;

sorted_triplets = ORDER triplets BY film_id;

DUMP sorted_triplets;
STORE sorted_triplets INTO 'pigout/movies/mUSA_acteurs' USING PigStorage(',');
--------------------------------------------------------------------------------------------

films = LOAD 'input/movies/films.json' 
    USING JsonLoader('
        _id:chararray,
        title:chararray,
        year:int,
        genre:chararray,
        summary:chararray,
        country:chararray,
        director:(id:chararray),
        actors:{t:(id:chararray,role:chararray)}
    ');

artists = LOAD 'input/movies/artists.json'
    USING JsonLoader('
        _id:chararray,
        last_name:chararray,
        first_name:chararray,
        birth_date:chararray
    ');

films_us = FILTER films BY country == 'US';

films_actors = FOREACH films_us GENERATE 
    _id AS film_id,
    title AS film_title,
    year,
    actors AS actors_list;

flattened_actors = FOREACH films_actors GENERATE 
    film_id,
    film_title,
    year,
    FLATTEN(actors_list) AS (actor_id:chararray, role:chararray);

joined = JOIN flattened_actors BY actor_id, artists BY _id;

movies_actors_full = FOREACH joined GENERATE 
    flattened_actors::film_id AS film_id,
    flattened_actors::film_title AS film_title,
    flattened_actors::year AS year,
    artists::_id AS actor_id,
    artists::first_name AS actor_first_name,
    artists::last_name AS actor_last_name,
    artists::birth_date AS actor_birth_date,
    flattened_actors::role AS role;

sorted_result = ORDER movies_actors_full BY film_id;

DUMP sorted_result;
STORE sorted_result INTO 'pigout/movies/moviesActors' USING PigStorage(',');
---------------------------------------------------------------------------------------

films = LOAD 'input/movies/films.json' 
    USING JsonLoader('
        _id:chararray,
        title:chararray,
        year:int,
        genre:chararray,
        summary:chararray,
        country:chararray,
        director:(id:chararray),
        actors:{t:(id:chararray,role:chararray)}
    ');

artists = LOAD 'input/movies/artists.json'
    USING JsonLoader('
        _id:chararray,
        last_name:chararray,
        first_name:chararray,
        birth_date:chararray
    ');

films_us = FILTER films BY country == 'US';

films_info = FOREACH films_us GENERATE 
    _id AS film_id,
    title,
    year,
    genre,
    director.id AS director_id;

films_actors_flat = FOREACH films_us GENERATE 
    _id AS film_id,
    FLATTEN(actors) AS (actor_id:chararray, role:chararray);

joined_actors = JOIN films_actors_flat BY actor_id, artists BY _id;

movies_actors = FOREACH joined_actors GENERATE 
    films_actors_flat::film_id AS film_id,
    artists::_id AS actor_id,
    artists::first_name AS actor_first,
    artists::last_name AS actor_last,
    films_actors_flat::role AS role;

cogrouped = COGROUP films_info BY film_id, movies_actors BY film_id;

full_movies = FOREACH cogrouped GENERATE 
    group AS film_id,
    FLATTEN(films_info.(title, year, genre, director_id)) AS (title, year, genre, director_id),
    movies_actors.(actor_first, actor_last, role) AS actors;

sorted_full_movies = ORDER full_movies BY year;

sample_full_movies = LIMIT sorted_full_movies 10;
DUMP sample_full_movies;

STORE sorted_full_movies INTO 'pigout/movies/fullMovies' USING PigStorage('|');
---------------------------------------------------------------------------------------

-- ============================================
-- Acteurs et Réalisateurs : Combinaison Films Joués / Dirigés
-- ============================================

-- Charger les films
films = LOAD 'input/movies/films.json' 
    USING JsonLoader('
        _id:chararray,
        title:chararray,
        year:int,
        genre:chararray,
        summary:chararray,
        country:chararray,
        director:(id:chararray),
        actors:{t:(id:chararray,role:chararray)}
    ');

-- Filtrer les films américains
films_us = FILTER films BY country == 'US';

-- =====================
-- Films dirigés par artiste
-- =====================
films_directed = FOREACH films_us GENERATE 
    director.id AS artist_id,
    _id AS film_id,
    title AS film_title;

directors_grouped = GROUP films_directed BY artist_id;
directors_list = FOREACH directors_grouped GENERATE 
    group AS artist_id,
    films_directed.(film_id, film_title) AS films_directed;

-- =====================
-- Films joués par artiste
-- =====================
films_acted_flat = FOREACH films_us GENERATE 
    _id AS film_id,
    title AS film_title,
    FLATTEN(actors) AS (actor_id:chararray, role:chararray);

films_acted = FOREACH films_acted_flat GENERATE 
    actor_id AS artist_id,
    film_id,
    film_title,
    role;

actors_grouped = GROUP films_acted BY artist_id;
actors_list = FOREACH actors_grouped GENERATE 
    group AS artist_id,
    films_acted.(film_id, film_title, role) AS films_acted;

-- =====================
-- COGROUP pour combiner
-- =====================
combined = COGROUP actors_list BY artist_id FULL OUTER, 
                        directors_list BY artist_id;

-- Formater le résultat final
artists_movies = FOREACH combined GENERATE
    group AS artist_id,
    (directors_list IS NULL OR SIZE(directors_list) == 0 ? TOBAG() : directors_list.films_directed) AS films_directed,
    (actors_list IS NULL OR SIZE(actors_list) == 0 ? TOBAG() : actors_list.films_acted) AS films_acted;

-- Afficher un échantillon
sample_result = LIMIT artists_movies 20;
DUMP sample_result;

-- Sauvegarder
STORE artists_movies INTO 'pigout/movies/ActeursRealisateurs' USING PigStorage('|');

