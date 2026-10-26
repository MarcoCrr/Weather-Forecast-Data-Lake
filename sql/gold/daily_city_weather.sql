SELECT
    city_name,
    CAST(forecast_timestamp AS DATE) AS forecast_date,
    ROUND(AVG(temp_celsius), 1) AS avg_temperature,
    ROUND(MIN(temp_celsius), 1) AS min_temperature,
    ROUND(MAX(temp_celsius), 1) AS max_temperature,
    ROUND(AVG(humidity_percent), 1) AS avg_humidity
FROM read_parquet(
    'data/silver/**/*.parquet',
    hive_partitioning = true
)
GROUP BY
    city_name,
    CAST(forecast_timestamp AS DATE)
ORDER BY
    forecast_date,
    avg_temperature DESC;