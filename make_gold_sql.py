import duckdb
from pathlib import Path


def save_gold_data(sql_file: str):
    connection = duckdb.connect()

    with open(sql_file, encoding="utf-8") as file:
        query = file.read()

    result = connection.execute(query).fetchdf()
    print(result)

    connection.close()


if __name__ == "__main__":

    Path("data/gold").mkdir(parents=True, exist_ok=True)
    
    path = ["sql/gold/city_temperature_rankings.sql",
            "sql/gold/daily_city_weather.sql",
            "sql/gold/weather_extremes.sql"]

    for sql_file in path:
        save_gold_data(sql_file)