flt = LOAD 'input/flights/sample_flights.csv' 
    USING PigStorage(',') 
    AS (
        year:int, month:int, day:int, day_of_week:int,
        dep_time:int, crs_dep_time:int, arr_time:int, crs_arr_time:int,
        carrier:chararray, flight_num:int, tail_num:chararray,
        actual_elapsed_time:int, crs_elapsed_time:int, air_time:int,
        arr_delay:int, dep_delay:int,
        origin:chararray, dest:chararray,
        distance:int, taxi_in:int, taxi_out:int,
        cancelled:int, cancellation_code:chararray, diverted:int,
        carrier_delay:int, weather_delay:int, nas_delay:int,
        security_delay:int, late_aircraft_delay:int
    );

flt_valid = FILTER flt BY cancelled == 0 AND year IS NOT NULL;

dep_air = FOREACH flt_valid GENERATE origin AS airport;
grp_dep = GROUP dep_air BY airport;
cnt_dep = FOREACH grp_dep GENERATE 
    group AS airport,
    COUNT(dep_air) AS dep_count;

arr_air = FOREACH flt_valid GENERATE dest AS airport;
grp_arr = GROUP arr_air BY airport;
cnt_arr = FOREACH grp_arr GENERATE 
    group AS airport,
    COUNT(arr_air) AS arr_count;

air_traffic = JOIN cnt_dep BY airport FULL OUTER, cnt_arr BY airport;

air_vol = FOREACH air_traffic GENERATE 
    (cnt_dep::airport IS NOT NULL ? cnt_dep::airport : cnt_arr::airport) AS airport,
    (cnt_dep::dep_count IS NOT NULL ? cnt_dep::dep_count : 0L) AS departures,
    (cnt_arr::arr_count IS NOT NULL ? cnt_arr::arr_count : 0L) AS arrivals,
    ((cnt_dep::dep_count IS NOT NULL ? cnt_dep::dep_count : 0L) + 
     (cnt_arr::arr_count IS NOT NULL ? cnt_arr::arr_count : 0L)) AS total_flights;

air_vol_sorted = ORDER air_vol BY total_flights DESC;

top20_air = LIMIT air_vol_sorted 20;

DUMP top20_air;

STORE top20_air INTO 'pigout/flights/top20_airports' USING PigStorage(',');

flt_year_orig = FOREACH flt_valid GENERATE year, origin AS airport;
flt_year_dest = FOREACH flt_valid GENERATE year, dest AS airport;

flt_year_all = UNION flt_year_orig, flt_year_dest;

grp_year_air = GROUP flt_year_all BY (year, airport);

vol_year_air = FOREACH grp_year_air GENERATE 
    FLATTEN(group) AS (year, airport),
    COUNT(flt_year_all) AS flight_count;

vol_year_sorted = ORDER vol_year_air BY year, flight_count DESC;

STORE vol_year_sorted INTO 'pigout/flights/airports_by_year' USING PigStorage(',');
-------------------------------------------------------------------------------------------

flt = LOAD 'input/flights/sample_flights.csv' 
    USING PigStorage(',') 
    AS (
        year:int, month:int, day:int, day_of_week:int,
        dep_time:int, crs_dep_time:int, arr_time:int, crs_arr_time:int,
        carrier:chararray, flight_num:int, tail_num:chararray,
        actual_elapsed_time:int, crs_elapsed_time:int, air_time:int,
        arr_delay:int, dep_delay:int,
        origin:chararray, dest:chararray,
        distance:int, taxi_in:int, taxi_out:int,
        cancelled:int, cancellation_code:chararray, diverted:int,
        carrier_delay:int, weather_delay:int, nas_delay:int,
        security_delay:int, late_aircraft_delay:int
    );

flt_valid = FILTER flt BY cancelled == 0 AND year IS NOT NULL;

carrier_yr = FOREACH flt_valid GENERATE year, carrier;

grp_carrier_yr = GROUP carrier_yr BY (year, carrier);

vol_carrier_yr = FOREACH grp_carrier_yr GENERATE 
    FLATTEN(group) AS (year, carrier),
    COUNT(carrier_yr) AS flight_count,
    LOG10((double)COUNT(carrier_yr)) AS log_volume;

vol_carrier_yr_sorted = ORDER vol_carrier_yr BY year, flight_count DESC;

DUMP vol_carrier_yr_sorted;

STORE vol_carrier_yr_sorted INTO 'pigout/flights/carrier_volume_by_year' USING PigStorage(',');

log_carrier = FOREACH vol_carrier_yr GENERATE carrier, log_volume;

grp_carrier = GROUP log_carrier BY carrier;

stats_carrier = FOREACH grp_carrier {
    sorted_vol = ORDER log_carrier BY log_volume;
    GENERATE 
        group AS carrier,
        COUNT(log_carrier) AS nb_years,
        AVG(log_carrier.log_volume) AS avg_log_volume,
        MIN(log_carrier.log_volume) AS min_log_volume,
        MAX(log_carrier.log_volume) AS max_log_volume;
}

stats_carrier_sorted = ORDER stats_carrier BY avg_log_volume DESC;

DUMP stats_carrier_sorted;

STORE stats_carrier_sorted INTO 'pigout/flights/carrier_popularity' USING PigStorage(',');
----------------------------------------------------------

flt = LOAD 'input/flights/sample_flights.csv' 
    USING PigStorage(',') 
    AS (
        year:int, month:int, day:int, day_of_week:int,
        dep_time:int, crs_dep_time:int, arr_time:int, crs_arr_time:int,
        carrier:chararray, flight_num:int, tail_num:chararray,
        actual_elapsed_time:int, crs_elapsed_time:int, air_time:int,
        arr_delay:int, dep_delay:int,
        origin:chararray, dest:chararray,
        distance:int, taxi_in:int, taxi_out:int,
        cancelled:int, cancellation_code:chararray, diverted:int,
        carrier_delay:int, weather_delay:int, nas_delay:int,
        security_delay:int, late_aircraft_delay:int
    );

flt_valid = FILTER flt BY cancelled == 0 AND year IS NOT NULL;

flt_delay = FOREACH flt_valid GENERATE 
    year,
    month,
    day,
    day_of_week,
    arr_delay,
    dep_delay,
    (arr_delay IS NOT NULL AND arr_delay > 15 ? 1 : 0) AS is_delayed;

grp_year = GROUP flt_delay BY year;

delay_year = FOREACH grp_year GENERATE 
    group AS year,
    COUNT(flt_delay) AS total_flights,
    SUM(flt_delay.is_delayed) AS delayed_flights,
    (double)SUM(flt_delay.is_delayed) / (double)COUNT(flt_delay) AS delay_proportion;

delay_year_sorted = ORDER delay_year BY year;

DUMP delay_year_sorted;
STORE delay_year_sorted INTO 'pigout/flights/delays_by_year' USING PigStorage(',');

grp_month = GROUP flt_delay BY (year, month);

delay_month = FOREACH grp_month GENERATE 
    FLATTEN(group) AS (year, month),
    COUNT(flt_delay) AS total_flights,
    SUM(flt_delay.is_delayed) AS delayed_flights,
    (double)SUM(flt_delay.is_delayed) / (double)COUNT(flt_delay) AS delay_proportion;

delay_month_sorted = ORDER delay_month BY year, month;

STORE delay_month_sorted INTO 'pigout/flights/delays_by_month' USING PigStorage(',');

grp_dow = GROUP flt_delay BY day_of_week;

delay_dow = FOREACH grp_dow GENERATE 
    group AS day_of_week,
    COUNT(flt_delay) AS total_flights,
    SUM(flt_delay.is_delayed) AS delayed_flights,
    (double)SUM(flt_delay.is_delayed) / (double)COUNT(flt_delay) AS delay_proportion;

delay_dow_sorted = ORDER delay_dow BY day_of_week;

DUMP delay_dow_sorted;
STORE delay_dow_sorted INTO 'pigout/flights/delays_by_day_of_week' USING PigStorage(',');

flt_hour = FOREACH flt_valid GENERATE 
    (dep_time IS NOT NULL ? (int)(dep_time / 100) : -1) AS dep_hour,
    (arr_delay IS NOT NULL AND arr_delay > 15 ? 1 : 0) AS is_delayed;

flt_hour_valid = FILTER flt_hour BY dep_hour >= 0 AND dep_hour < 24;

grp_hour = GROUP flt_hour_valid BY dep_hour;

delay_hour = FOREACH grp_hour GENERATE 
    group AS hour,
    COUNT(flt_hour_valid) AS total_flights,
    SUM(flt_hour_valid.is_delayed) AS delayed_flights,
    (double)SUM(flt_hour_valid.is_delayed) / (double)COUNT(flt_hour_valid) AS delay_proportion;

delay_hour_sorted = ORDER delay_hour BY hour;

DUMP delay_hour_sorted;
STORE delay_hour_sorted INTO 'pigout/flights/delays_by_hour' USING PigStorage(',');
-------------------------------------------------------------------------------------------------
flt = LOAD 'input/flights/sample_flights.csv' 
    USING PigStorage(',') 
    AS (
        year:int, month:int, day:int, day_of_week:int,
        dep_time:int, crs_dep_time:int, arr_time:int, crs_arr_time:int,
        carrier:chararray, flight_num:int, tail_num:chararray,
        actual_elapsed_time:int, crs_elapsed_time:int, air_time:int,
        arr_delay:int, dep_delay:int,
        origin:chararray, dest:chararray,
        distance:int, taxi_in:int, taxi_out:int,
        cancelled:int, cancellation_code:chararray, diverted:int,
        carrier_delay:int, weather_delay:int, nas_delay:int,
        security_delay:int, late_aircraft_delay:int
    );

flt_valid = FILTER flt BY cancelled == 0 AND year IS NOT NULL;

flt_carrier = FOREACH flt_valid GENERATE 
    carrier,
    year,
    month,
    day_of_week,
    (arr_delay IS NOT NULL AND arr_delay > 15 ? 1 : 0) AS is_delayed;

grp_carrier = GROUP flt_carrier BY carrier;

delay_carrier_total = FOREACH grp_carrier GENERATE 
    group AS carrier,
    COUNT(flt_carrier) AS total_flights,
    SUM(flt_carrier.is_delayed) AS delayed_flights,
    (double)SUM(flt_carrier.is_delayed) / (double)COUNT(flt_carrier) AS delay_rate;

sorted_carrier_total = ORDER delay_carrier_total BY delay_rate DESC;

DUMP sorted_carrier_total;
STORE sorted_carrier_total INTO 'pigout/flights/carrier_delays_total' USING PigStorage(',');

grp_carrier_year = GROUP flt_carrier BY (carrier, year);

delay_carrier_year = FOREACH grp_carrier_year GENERATE 
    FLATTEN(group) AS (carrier, year),
    COUNT(flt_carrier) AS total_flights,
    SUM(flt_carrier.is_delayed) AS delayed_flights,
    (double)SUM(flt_carrier.is_delayed) / (double)COUNT(flt_carrier) AS delay_rate;

sorted_carrier_year = ORDER delay_carrier_year BY year, delay_rate DESC;

STORE sorted_carrier_year INTO 'pigout/flights/carrier_delays_by_year' USING PigStorage(',');

grp_carrier_month = GROUP flt_carrier BY (carrier, year, month);

delay_carrier_month = FOREACH grp_carrier_month GENERATE 
    FLATTEN(group) AS (carrier, year, month),
    COUNT(flt_carrier) AS total_flights,
    SUM(flt_carrier.is_delayed) AS delayed_flights,
    (double)SUM(flt_carrier.is_delayed) / (double)COUNT(flt_carrier) AS delay_rate;

sorted_carrier_month = ORDER delay_carrier_month BY year, month, delay_rate DESC;

STORE sorted_carrier_month INTO 'pigout/flights/carrier_delays_by_month' USING PigStorage(',');

grp_carrier_dow = GROUP flt_carrier BY (carrier, day_of_week);

delay_carrier_dow = FOREACH grp_carrier_dow GENERATE 
    FLATTEN(group) AS (carrier, day_of_week),
    COUNT(flt_carrier) AS total_flights,
    SUM(flt_carrier.is_delayed) AS delayed_flights,
    (double)SUM(flt_carrier.is_delayed) / (double)COUNT(flt_carrier) AS delay_rate;

sorted_carrier_dow = ORDER delay_carrier_dow BY day_of_week, delay_rate DESC;

STORE sorted_carrier_dow INTO 'pigout/flights/carrier_delays_by_dow' USING PigStorage(',');

top10_worst_carriers = LIMIT sorted_carrier_total 10;

DUMP top10_worst_carriers;
------------------------------------------------------------------------------------------
flt = LOAD 'input/flights/sample_flights.csv' 
    USING PigStorage(',') 
    AS (
        year:int, month:int, day:int, day_of_week:int,
        dep_time:int, crs_dep_time:int, arr_time:int, crs_arr_time:int,
        carrier:chararray, flight_num:int, tail_num:chararray,
        actual_elapsed_time:int, crs_elapsed_time:int, air_time:int,
        arr_delay:int, dep_delay:int,
        origin:chararray, dest:chararray,
        distance:int, taxi_in:int, taxi_out:int,
        cancelled:int, cancellation_code:chararray, diverted:int,
        carrier_delay:int, weather_delay:int, nas_delay:int,
        security_delay:int, late_aircraft_delay:int
    );

flt_valid = FILTER flt BY cancelled == 0 AND year IS NOT NULL;

routes = FOREACH flt_valid GENERATE 
    (origin < dest ? origin : dest) AS a1,
    (origin < dest ? dest : origin) AS a2,
    origin,
    dest,
    distance;

grp_routes = GROUP routes BY (a1, a2);

freq_routes = FOREACH grp_routes GENERATE 
    FLATTEN(group) AS (a1, a2),
    COUNT(routes) AS flights,
    AVG(routes.distance) AS avg_dist;

sorted_routes = ORDER freq_routes BY flights DESC;

top20_routes = LIMIT sorted_routes 20;

DUMP top20_routes;
STORE top20_routes INTO 'pigout/flights/popular_routes' USING PigStorage(',');

dir_routes = FOREACH flt_valid GENERATE origin, dest, distance;

grp_dir = GROUP dir_routes BY (origin, dest);

freq_dir = FOREACH grp_dir GENERATE 
    FLATTEN(group) AS (origin, dest),
    COUNT(dir_routes) AS flights,
    AVG(dir_routes.distance) AS avg_dist;

sorted_dir = ORDER freq_dir BY flights DESC;

top20_dir = LIMIT sorted_dir 20;

DUMP top20_dir;
STORE top20_dir INTO 'pigout/flights/popular_directional_routes' USING PigStorage(',');

carrier_routes = FOREACH flt_valid GENERATE carrier, origin, dest;

grp_carrier_routes = GROUP carrier_routes BY (carrier, origin, dest);

freq_carrier = FOREACH grp_carrier_routes GENERATE 
    FLATTEN(group) AS (carrier, origin, dest),
    COUNT(carrier_routes) AS flights;

sorted_carrier = ORDER freq_carrier BY flights DESC;

top20_carrier = LIMIT sorted_carrier 20;

DUMP top20_carrier;
STORE top20_carrier INTO 'pigout/flights/popular_carrier_routes' USING PigStorage(',');

routes_dist = FOREACH flt_valid GENERATE origin, dest, distance;

unique_routes = DISTINCT routes_dist;

sorted_dist = ORDER unique_routes BY distance DESC;

top20_long = LIMIT sorted_dist 20;

DUMP top20_long;
STORE top20_long INTO 'pigout/flights/longest_routes' USING PigStorage(',');

