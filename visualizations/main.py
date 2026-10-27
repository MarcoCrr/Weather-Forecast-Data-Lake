import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns

from .data_loader import load_temperature_rankings
from .plots import (
    plot_temperature_rankings,
    plot_temperature_trends,
    plot_temperature_heatmap,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "visualizations" / "output"


def main():
    parser = argparse.ArgumentParser(
        description="Generate weather forecast visualizations."
    )

    parser.add_argument(
        "--data",
        type=Path,
        default=None,
        help="Path to the Gold Parquet dataset.",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where plots will be saved.",
    )

    parser.add_argument(
        "--show",
        action="store_true",
        help="Display plots after saving them.",
    )

    args = parser.parse_args()

    sns.set_theme(style="whitegrid")

    df = load_temperature_rankings(
        args.data if args.data is not None
        else PROJECT_ROOT
        / "data"
        / "gold"
        / "city_temperature_rankings.parquet"
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)

    figures = {
        "temperature_rankings.png": plot_temperature_rankings(df),
        "temperature_trends.png": plot_temperature_trends(df),
        "temperature_heatmap.png": plot_temperature_heatmap(df),
    }

    for filename, fig in figures.items():
        output_path = args.output_dir / filename

        fig.savefig(
            output_path,
            dpi=150,
            bbox_inches="tight",
        )

        print(f"Saved: {output_path}")

        if args.show:
            plt.figure(fig.number)

    if args.show:
        plt.show()

    for fig in figures.values():
        plt.close(fig)


if __name__ == "__main__":
    main()