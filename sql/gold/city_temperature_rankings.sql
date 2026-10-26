WITH daily_stats AS (
    SELECT
        city_name,
        CAST(forecast_timestamp AS DATE) AS forecast_date,
        AVG(temp_celsius) AS avg_temperature
    FROM read_parquet(
        'data/silver/**/*.parquet',
        hive_partitioning = true
    )
    GROUP BY
        city_name,
        CAST(forecast_timestamp AS DATE)
)
SELECT
    forecast_date,
    city_name,
    ROUND(avg_temperature, 1) AS avg_temperature,
    RANK() OVER (
        PARTITION BY forecast_date
        ORDER BY avg_temperature DESC
    ) AS temperature_rank
FROM daily_stats
ORDER BY forecast_date, temperature_rank;