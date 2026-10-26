import duckdb

def main():
    connection = duckdb.connect()

    with open("sql/exploratory/inspect_silver.sql", encoding="utf-8") as file:
        query = file.read()

    result = connection.execute(query).fetchdf()
    print(result)

    connection.close()


if __name__ == "__main__":
    main()