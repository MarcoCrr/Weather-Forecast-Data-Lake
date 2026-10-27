import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def plot_temperature_rankings(
    df: pd.DataFrame,
    forecast_date=None,
    top_n: int | None = None,
):
    """Compare average forecast temperatures across cities."""
    if forecast_date is None:
        forecast_date = df["forecast_date"].max()

    forecast_date = pd.Timestamp(forecast_date).normalize()

    daily_df = df[
        df["forecast_date"] == forecast_date
    ].copy()

    if daily_df.empty:
        raise ValueError(
            f"No data available for {forecast_date.date()}."
        )

    daily_df = daily_df.sort_values(
        "avg_temperature", ascending=False
    )

    if top_n is not None:
        if top_n < 1:
            raise ValueError("top_n must be at least 1.")

        daily_df = daily_df.head(top_n)

    fig, ax = plt.subplots(figsize=(12, 8))

    sns.barplot(
        data=daily_df,
        x="avg_temperature",
        y="city_name",
        hue="city_name",
        palette="coolwarm_r",
        legend=False,
        ax=ax,
    )

    ax.set_title(
        f"Average Forecast Temperature by City — "
        f"{forecast_date:%Y-%m-%d}"
    )
    ax.set_xlabel("Average temperature (°C)")
    ax.set_ylabel("City")

    fig.tight_layout()

    return fig


def plot_temperature_trends(
    df: pd.DataFrame,
    cities: list[str] | None = None,
):
    """Plot daily average forecast temperatures for selected cities."""
    trend_df = df.copy()

    if cities is not None:
        trend_df = trend_df[
            trend_df["city_name"].isin(cities)
        ]

        missing_cities = set(cities) - set(
            trend_df["city_name"].unique()
        )

        if missing_cities:
            raise ValueError(
                f"No data found for cities: "
                f"{sorted(missing_cities)}"
            )
    else:
        # Select five cities with the most available dates.
        selected_cities = (
            trend_df.groupby("city_name")["forecast_date"]
            .nunique()
            .nlargest(5)
            .index
        )

        trend_df = trend_df[
            trend_df["city_name"].isin(selected_cities)
        ]

    trend_df = trend_df.sort_values("forecast_date")

    fig, ax = plt.subplots(figsize=(12, 6))

    sns.lineplot(
        data=trend_df,
        x="forecast_date",
        y="avg_temperature",
        hue="city_name",
        marker="o",
        ax=ax,
    )

    ax.set_title("Forecast Temperature Trends by City")
    ax.set_xlabel("Forecast date")
    ax.set_ylabel("Average temperature (°C)")

    fig.autofmt_xdate()
    fig.tight_layout()

    return fig


def plot_temperature_heatmap(
    df: pd.DataFrame,
):
    """Compare average forecast temperatures across cities and dates."""
    heatmap_df = df.pivot_table(
        index="city_name",
        columns="forecast_date",
        values="avg_temperature",
        aggfunc="mean",
    )

    if heatmap_df.empty:
        raise ValueError("No data available for the heatmap.")

    heatmap_df = heatmap_df.sort_index(axis=1)

    fig, ax = plt.subplots(figsize=(14, 10))

    sns.heatmap(
        heatmap_df,
        cmap="coolwarm",
        annot=False,
        linewidths=0.2,
        ax=ax,
        cbar_kws={"label": "Average temperature (°C)"},
    )

    ax.set_title("Forecast Temperature Across Cities and Dates")
    ax.set_xlabel("Forecast date")
    ax.set_ylabel("City")
    ax.set_xticklabels(
    [date.strftime("%Y-%m-%d") for date in heatmap_df.columns],
    rotation=45,
    ha="right",
    )

    fig.tight_layout()

    return fig