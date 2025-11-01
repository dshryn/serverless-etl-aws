-- monthly summary
CREATE OR REPLACE VIEW airport_monthly_summary AS
SELECT
airport_code,
airport_name,
year,
month,
SUM(flights_total) AS total_flights,
SUM(flights_delayed) AS total_delayed,
SUM(flights_cancelled) AS total_cancelled,
SUM(minutes_total) AS total_delay_minutes,
SUM(minutes_weather) AS total_weather_minutes,
SUM(minutes_carrier) AS total_carrier_minutes,
SUM(minutes_late_aircraft) AS total_late_aircraft_minutes,
SUM(minutes_national_system) AS total_nas_minutes,
ROUND( SUM(flights_delayed) * 100.0 / NULLIF(SUM(flights_total),0), 2) AS pct_delayed,
ROUND( SUM(minutes_total) * 1.0 / NULLIF(SUM(flights_delayed),0), 2) AS avg_minutes_per_delayed_flight
FROM airport_delays_parquet
GROUP BY airport_code, airport_name, year, month;

-- main kpi
CREATE OR REPLACE VIEW airport_kpi AS
SELECT
airport_code,
airport_name,
year,
month,
SUM(flights_total) AS total_flights,
SUM(flights_delayed) AS total_delayed,
SUM(flights_cancelled) AS total_cancelled,
SUM(minutes_total) AS total_delay_minutes,
CASE WHEN SUM(flights_total)=0 THEN 0
    ELSE ROUND(SUM(flights_delayed)*100.0/SUM(flights_total),2) END AS pct_delayed,
CASE WHEN SUM(flights_delayed)=0 THEN NULL
    ELSE ROUND(SUM(minutes_total)*1.0/SUM(flights_delayed),2) END AS avg_minutes_per_delayed_flight
FROM airport_delays_parquet
GROUP BY airport_code, airport_name, year, month;






