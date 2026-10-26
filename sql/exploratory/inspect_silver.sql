-- Inspect the Silver dataset

SELECT *
FROM read_parquet(
    'data/silver/**/*.parquet',
    hive_partitioning = true
)
LIMIT 20;