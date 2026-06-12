from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

sns.set_theme(style="whitegrid")


# Wykres średniego GHI po miesiącach.
def plot_monthly_ghi(monthly_df: pd.DataFrame, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=monthly_df, x="Month", y="avg_ghi", color="#f4b400", ax=ax)
    ax.set_title("Średnie GHI według miesiąca")
    ax.set_xlabel("Miesiąc")
    ax.set_ylabel("Średnie GHI")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


# Wykres czasu dla eksperymentu baseline i optimized.
def plot_experiment_times(metrics_df: pd.DataFrame, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(7, 4))
    sns.barplot(data=metrics_df, x="scenario", y="total_pipeline_time_s", color="#4c78a8", ax=ax)
    ax.set_title("Czas całego pipeline")
    ax.set_xlabel("Scenariusz")
    ax.set_ylabel("Sekundy")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


# Wykres korelacji cech z Relative Humidity.
def plot_relative_humidity_correlations(corr_df: pd.DataFrame, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(9, 4.5))
    sns.barplot(
        data=corr_df,
        x="feature",
        y="correlation_with_relative_humidity",
        color="#2a9d8f",
        ax=ax,
    )
    ax.set_title("Korelacje z Relative Humidity")
    ax.set_xlabel("Cecha")
    ax.set_ylabel("Współczynnik korelacji")
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


# Wykres porównujący czas pracy na bronze i silver.
def plot_bronze_vs_silver_times(metrics_df: pd.DataFrame, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(7, 4))
    sns.barplot(data=metrics_df, x="layer", y="total_time_s", color="#8d99ae", ax=ax)
    ax.set_title("Bronze vs silver: czas tych samych operacji")
    ax.set_xlabel("Warstwa")
    ax.set_ylabel("Sekundy")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path
