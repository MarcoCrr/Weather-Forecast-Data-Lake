import os
import sys
import logging
import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Script path constants
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_BRONZE_DIR = os.path.join(SCRIPT_DIR, "data", "bronze")


def get_spark_session(app_name: str = "WeatherBronzeToSilver") -> SparkSession:
    """
    Creates or retrieves an optimized PySpark session.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.hadoop.hadoop.security.authentication", "simple")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def get_openweather_schema() -> T.StructType:
    """
    Returns explicit schema definition for standard OpenWeather API responses.
    Using explicit schemas improves performance by preventing schema inference passes.
    """
    coord_schema = T.StructType([
        T.StructField("lon", T.DoubleType(), True),
        T.StructField("lat", T.DoubleType(), True)
    ])

    weather_item_schema = T.StructType([
        T.StructField("id", T.LongType(), True),
        T.StructField("main", T.StringType(), True),
        T.StructField("description", T.StringType(), True),
        T.StructField("icon", T.StringType(), True)
    ])

    main_schema = T.StructType([
        T.StructField("temp", T.DoubleType(), True),
        T.StructField("feels_like", T.DoubleType(), True),
        T.StructField("temp_min", T.DoubleType(), True),
        T.StructField("temp_max", T.DoubleType(), True),
        T.StructField("pressure", T.IntegerType(), True),
        T.StructField("humidity", T.IntegerType(), True)
    ])

    wind_schema = T.StructType([
        T.StructField("speed", T.DoubleType(), True),
        T.StructField("deg", T.IntegerType(), True),
        T.StructField("gust", T.DoubleType(), True)
    ])

    clouds_schema = T.StructType([
        T.StructField("all", T.IntegerType(), True)
    ])

    sys_schema = T.StructType([
        T.StructField("type", T.IntegerType(), True),
        T.StructField("id", T.LongType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("sunrise", T.LongType(), True),
        T.StructField("sunset", T.LongType(), True)
    ])

    city_schema = T.StructType([
        T.StructField("id", T.LongType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("coord", coord_schema, True),
        T.StructField("country", T.StringType(), True),
        T.StructField("population", T.LongType(), True),
        T.StructField("timezone", T.IntegerType(), True),
        T.StructField("sunrise", T.LongType(), True),
        T.StructField("sunset", T.LongType(), True)
    ])

    # Structure matching standard current weather or forecast-aggregated payload
    payload_schema = T.StructType([
        T.StructField("coord", coord_schema, True),
        T.StructField("weather", T.ArrayType(weather_item_schema), True),
        T.StructField("main", main_schema, True),
        T.StructField("wind", wind_schema, True),
        T.StructField("clouds", clouds_schema, True),
        T.StructField("dt", T.LongType(), True),
        T.StructField("sys", sys_schema, True),
        T.StructField("timezone", T.IntegerType(), True),
        T.StructField("id", T.LongType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("cod", T.LongType(), True),
        # Fields present in forecast hourly city payloads
        T.StructField("city", city_schema, True),
        T.StructField("weather_data", T.StructType([
            T.StructField("main", main_schema, True),
            T.StructField("weather", T.ArrayType(weather_item_schema), True),
            T.StructField("clouds", clouds_schema, True),
            T.StructField("wind", wind_schema, True),
            T.StructField("dt", T.LongType(), True)
        ]), True)
    ])

    return payload_schema


def read_bronze_data(spark: SparkSession, bronze_dir: str):
    """
    Reads all raw JSON files under the Bronze directory recursively using Spark.
    Filters to *.json files to ignore non-JSON files like .gitkeep.
    Enables multiLine parsing for pretty-printed JSON records.
    """
    logger.info(f"Reading bronze JSON data from path: {bronze_dir}")
    
    # Enable recursive file lookup for partitioned folders (year/month/day/hour)
    spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
    
    raw_df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .option("multiLine", "true")
        .json(bronze_dir)
    )
    return raw_df


def transform_bronze_to_silver(df):
    """
    Transforms raw JSON DataFrame into a cleaned, flattened, standardized Silver DataFrame.
    """
    # Exclude internal corrupt record column if present
    field_names = [col for col in df.columns if col != "_corrupt_record"]

    if not field_names:
        raise ValueError("No valid data columns found in Bronze JSON files. Ensure files contain valid JSON.")

    if df.rdd.isEmpty():
        logger.warning("Input DataFrame contains no records.")
        return df
    
    # Build dynamic expression to stack dynamic city columns into rows
    stack_exprs = []
    for col_name in field_names:
        # Sanitize column name escaping for backticks
        escaped_col = f"`{col_name}`"
        stack_exprs.append(f"'{col_name}', {escaped_col}")

    stack_str = f"stack({len(field_names)}, {', '.join(stack_exprs)}) as (raw_city_key, payload)"
    
    flattened_df = df.select(F.expr(stack_str)).filter(F.col("payload").isNotNull())

    # Extract nested payload metrics (handling both current weather and forecast payloads)
    transformed = flattened_df.select(
        F.col("raw_city_key").alias("city_key"),
        F.coalesce(
            F.col("payload.name"),
            F.col("payload.city.name"),
            F.col("raw_city_key")
        ).alias("city_name"),
        F.coalesce(
            F.col("payload.id"),
            F.col("payload.city.id")
        ).alias("city_id"),
        F.coalesce(
            F.col("payload.sys.country"),
            F.col("payload.city.country")
        ).alias("country"),
        F.coalesce(
            F.col("payload.coord.lat"),
            F.col("payload.city.coord.lat")
        ).alias("latitude"),
        F.coalesce(
            F.col("payload.coord.lon"),
            F.col("payload.city.coord.lon")
        ).alias("longitude"),
        F.coalesce(
            F.col("payload.dt"),
            F.col("payload.weather_data.dt")
        ).alias("observation_timestamp_epoch"),
        F.coalesce(
            F.col("payload.main.temp"),
            F.col("payload.weather_data.main.temp")
        ).alias("temp_celsius"),
        F.coalesce(
            F.col("payload.main.feels_like"),
            F.col("payload.weather_data.main.feels_like")
        ).alias("feels_like_celsius"),
        F.coalesce(
            F.col("payload.main.temp_min"),
            F.col("payload.weather_data.main.temp_min")
        ).alias("temp_min_celsius"),
        F.coalesce(
            F.col("payload.main.temp_max"),
            F.col("payload.weather_data.main.temp_max")
        ).alias("temp_max_celsius"),
        F.coalesce(
            F.col("payload.main.pressure"),
            F.col("payload.weather_data.main.pressure")
        ).alias("pressure_hpa"),
        F.coalesce(
            F.col("payload.main.humidity"),
            F.col("payload.weather_data.main.humidity")
        ).alias("humidity_percent"),
        F.coalesce(
            F.col("payload.wind.speed"),
            F.col("payload.weather_data.wind.speed")
        ).alias("wind_speed_m_s"),
        F.coalesce(
            F.col("payload.wind.deg"),
            F.col("payload.weather_data.wind.deg")
        ).alias("wind_deg"),
        F.coalesce(
            F.col("payload.clouds.all"),
            F.col("payload.weather_data.clouds.all")
        ).alias("cloudiness_percent"),
        F.element_at(
            F.coalesce(
                F.col("payload.weather.main"),
                F.col("payload.weather_data.weather.main")
            ), 1
        ).alias("weather_condition"),
        F.element_at(
            F.coalesce(
                F.col("payload.weather.description"),
                F.col("payload.weather_data.weather.description")
            ), 1
        ).alias("weather_description")
    )

    # Convert timestamp epoch to explicit UTC Timestamp
    transformed = transformed.withColumn(
        "observation_timestamp",
        F.to_timestamp(F.from_unixtime(F.col("observation_timestamp_epoch")))
    )

    # Add partition metadata columns derived from observation time
    transformed = (
        transformed
        .withColumn("year", F.date_format("observation_timestamp", "yyyy"))
        .withColumn("month", F.date_format("observation_timestamp", "MM"))
        .withColumn("day", F.date_format("observation_timestamp", "dd"))
        .withColumn("hour", F.date_format("observation_timestamp", "HH"))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    return transformed


def validate_and_clean_data(df):
    """
    Applies data quality rules and deduplication to guarantee Silver standard records.
    """
    logger.info("Applying quality validations and deduplication...")

    # Filter invalid records (null timestamp, missing city, or unphysical temperature values)
    cleaned_df = df.filter(
        F.col("city_name").isNotNull() &
        F.col("observation_timestamp").isNotNull() &
        F.col("temp_celsius").between(-100.0, 70.0) &
        F.col("humidity_percent").between(0, 100)
    )

    # Deduplicate entries by city name and exact observation timestamp
    deduped_df = cleaned_df.dropDuplicates(["city_name", "observation_timestamp"])

    return deduped_df


def write_silver_data(df, silver_dir: str):
    """
    Writes the Silver dataset into Parquet format partitioned by year/month/day/hour.
    """
    logger.info(f"Writing Silver Parquet files to: {silver_dir}")

    (
        df.write
        .mode("overwrite")
        .partitionBy("year", "month", "day", "hour")
        .option("compression", "snappy")
        .parquet(silver_dir)
    )

    logger.info("Successfully completed writing Silver dataset.")


def main():
    parser = argparse.ArgumentParser(description="PySpark Bronze to Silver Weather ETL Pipeline")
    parser.add_argument(
        "--bronze-dir",
        default=DEFAULT_BRONZE_DIR,
        help="Path to input raw Bronze directory."
    )
    parser.add_argument(
        "--silver-dir",
        default=None,
        help="Path to output Silver Parquet directory. Defaults to 'silver' directory alongside bronze-dir."
    )

    args = parser.parse_args()

    # Determine silver directory in same parent directory as bronze if not explicitly provided
    bronze_dir = os.path.abspath(args.bronze_dir)
    if args.silver_dir:
        silver_dir = os.path.abspath(args.silver_dir)
    else:
        parent_dir = os.path.dirname(bronze_dir)
        silver_dir = os.path.join(parent_dir, "silver")

    spark = get_spark_session()

    try:
        raw_df = read_bronze_data(spark, bronze_dir)
        transformed_df = transform_bronze_to_silver(raw_df)
        cleaned_df = validate_and_clean_data(transformed_df)
        
        record_count = cleaned_df.count()
        logger.info(f"Processed {record_count} valid records for Silver storage.")

        if record_count > 0:
            write_silver_data(cleaned_df, silver_dir)
        else:
            logger.warning("No records were written to Silver layer.")

    except Exception as err:
        logger.error(f"ETL Execution failed: {err}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
