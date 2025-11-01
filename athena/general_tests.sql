-- top airports by total delay minutes for a yr
SELECT airport_code, airport_name, SUM(minutes_total) AS total_delay_minutes
FROM airport_delays_parquet
WHERE year = 2006
GROUP BY airport_code, airport_name
ORDER BY total_delay_minutes DESC
LIMIT 35;

-- explode carriers into rows
WITH exploded AS (
SELECT airport_code, airport_name, year, month, carriers_names
FROM airport_delays_parquet
WHERE carriers_names IS NOT NULL
)
SELECT airport_code,
airport_name,
year,
month,
trim(c) AS carrier_name
FROM exploded
CROSS JOIN UNNEST(split(carriers_names, ',')) AS t(c)
;

-- monthly trend total flights / delays / delay minutes across all airports
SELECT year, month,
SUM(flights_total) AS total_flights,
SUM(flights_delayed) AS total_delays,
SUM(minutes_total) AS total_delay_minutes
FROM airport_delays_parquet
GROUP BY year, month
ORDER BY year, month;

-- top airports by weather-caused mins for a yr
SELECT airport_code, airport_name, SUM(minutes_weather) AS weather_minutes
FROM airport_delays_parquet
WHERE year = 2008
GROUP BY airport_code, airport_name
ORDER BY weather_minutes DESC
LIMIT 20;

-- top airports by delay rate
SELECT airport_code, airport_name,
SUM(flights_total) AS total_flights,
SUM(flights_delayed) AS total_delayed,
ROUND(SUM(flights_delayed)*100.0/NULLIF(SUM(flights_total),0),2) AS pct_delayed
FROM airport_delays_parquet
WHERE year = 2003
GROUP BY airport_code, airport_name
ORDER BY pct_delayed DESC
LIMIT 10;

