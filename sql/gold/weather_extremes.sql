SELECT
    city_name,
    forecast_timestamp,
    temp_celsius,
    wind_speed_m_s,
    precipitation_probability
FROM read_parquet(
    'data/silver/**/*.parquet',
    hive_partitioning = true
)
WHERE
    temp_celsius >= 35
    OR wind_speed_m_s >= 15
    OR precipitation_probability >= 0.8
ORDER BY forecast_timestamp;