from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_DATA_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
    / "city_temperature_rankings.parquet"
)

REQUIRED_COLUMNS = {
    "forecast_date",
    "city_name",
    "avg_temperature",
    "temperature_rank",
}


def load_temperature_rankings(
    path: str | Path = DEFAULT_DATA_PATH,
) -> pd.DataFrame:
    """Load and validate the Gold temperature rankings dataset."""
    path = Path(path)

    if not path.is_file():
        raise FileNotFoundError(
            f"Gold dataset not found: {path}"
        )

    df = pd.read_parquet(path)

    missing_columns = REQUIRED_COLUMNS - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Dataset is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    if df.empty:
        raise ValueError("The Gold dataset contains no rows.")

    df = df.copy()

    df["forecast_date"] = pd.to_datetime(
        df["forecast_date"], errors="raise"
    ).dt.normalize()

    df["avg_temperature"] = pd.to_numeric(
        df["avg_temperature"], errors="raise"
    )

    df["temperature_rank"] = pd.to_numeric(
        df["temperature_rank"], errors="raise"
    )

    if df["city_name"].isna().any():
        raise ValueError("The dataset contains missing city names.")

    if df["avg_temperature"].isna().any():
        raise ValueError("The dataset contains missing temperatures.")

    return df