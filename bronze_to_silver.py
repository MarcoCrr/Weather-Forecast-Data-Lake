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


def read_bronze_data(spark: SparkSession, bronze_dir: str):
    """
    Reads all raw JSON files under the Bronze directory recursively using Spark.
    Filters to *.json files to ignore non-JSON files.
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
    Flattens Bronze records into the Silver analytical model.
    One row = one city forecast.
    """

    if df.rdd.isEmpty():
        logger.warning("Input DataFrame contains no records.")
        return df

    silver_df = df.select(
        # Metadata
        F.col("metadata.city_name").alias("city_name"),
        F.col("metadata.source.provider").alias("provider"),
        F.col("metadata.source.endpoint").alias("endpoint"),
        F.to_timestamp("metadata.ingestion_timestamp").alias("ingestion_timestamp"),
        F.to_timestamp("metadata.forecast_timestamp").alias("forecast_timestamp"),

        # City
        F.col("city.id").alias("city_id"),
        F.col("city.country").alias("country"),
        F.col("city.coord.lat").alias("latitude"),
        F.col("city.coord.lon").alias("longitude"),
        F.col("city.population").alias("population"),
        F.col("city.timezone").alias("timezone_offset_seconds"),

        # Weather metrics
        F.col("payload.main.temp").alias("temp_celsius"),
        F.col("payload.main.feels_like").alias("feels_like_celsius"),
        F.col("payload.main.temp_min").alias("temp_min_celsius"),
        F.col("payload.main.temp_max").alias("temp_max_celsius"),
        F.col("payload.main.pressure").alias("pressure_hpa"),
        F.col("payload.main.humidity").alias("humidity_percent"),
        F.col("payload.main.sea_level").alias("sea_level_hpa"),
        F.col("payload.main.grnd_level").alias("ground_level_hpa"),
        F.col("payload.main.dew_point").alias("dew_point_celsius"),

        # Wind
        F.col("payload.wind.speed").alias("wind_speed_m_s"),
        F.col("payload.wind.deg").alias("wind_direction_deg"),
        F.col("payload.wind.gust").alias("wind_gust_m_s"),

        # Clouds & visibility
        F.col("payload.clouds.all").alias("cloud_cover_percent"),
        F.col("payload.visibility").alias("visibility_m"),

        # Rain probability
        F.col("payload.pop").alias("precipitation_probability"),

        # Weather description
        F.element_at("payload.weather.main", 1).alias("weather_main"),
        F.element_at("payload.weather.description", 1).alias("weather_description"),

        # Original epoch
        F.col("payload.dt").alias("forecast_epoch")
    )

    # Partition columns
    silver_df = (
        silver_df
        .withColumn("year", F.date_format("forecast_timestamp", "yyyy"))
        .withColumn("month", F.date_format("forecast_timestamp", "MM"))
        .withColumn("day", F.date_format("forecast_timestamp", "dd"))
        .withColumn("hour", F.date_format("forecast_timestamp", "HH"))
    )

    return silver_df


def validate_and_clean_data(df):
    """
    Applies data quality rules and deduplication to guarantee Silver standard records.
    """
    logger.info("Applying quality validations and deduplication...")

    # Filter invalid records (null timestamp, missing city, or unphysical temperature values)
    cleaned_df = df.filter(
        F.col("city_name").isNotNull() &
        F.col("forecast_timestamp").isNotNull() &
        F.col("temp_celsius").between(-100.0, 70.0) &
        F.col("humidity_percent").between(0, 100)
    )

    # Deduplicate entries by city name and exact forecast timestamp
    deduped_df = cleaned_df.dropDuplicates(["city_name", "forecast_timestamp"])

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
