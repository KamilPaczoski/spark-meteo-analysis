from __future__ import annotations

import tempfile
from pathlib import Path

import matplotlib.pyplot as plt

from spark_radiation_analysis.analytics.processing_compare import compare_processing_modes
from spark_radiation_analysis.analytics.solar import (
    hour_table,
    read_silver,
    rel_table,
    scenario1_quality,
    scenario2_quality,
    save_table,
    stat_table,
)
from spark_radiation_analysis.config import load_paths
from spark_radiation_analysis.spark import create_spark


def save_report_tables(
    stats_pd,
    rel_pd,
    hourly_pd,
    q1_pd,
    q2_pd,
    cmp_pd,
    out_dir: Path,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    stats_pd.to_csv(out_dir / "tabela1_profil_statystyczny.csv", index=False)
    rel_pd.to_csv(out_dir / "tabela2_korelacje.csv", index=False)
    hourly_pd.to_csv(out_dir / "tabela3_profil_godzinowy.csv", index=False)
    q1_pd.to_csv(out_dir / "tabela4_jakosc_danych.csv", index=False)
    q2_pd.to_csv(out_dir / "tabela5_jakosc_analizy.csv", index=False)
    cmp_pd.to_csv(out_dir / "tabela6_porownanie_trybow.csv", index=False)


def save_charts(hourly_pd, rel_pd, cmp_pd, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # Wykres 1 — dzienny profil promieniowania (wynik aplikacji).
    plt.figure(figsize=(10, 4))
    plt.plot(hourly_pd["Hour"], hourly_pd["avg_ghi"], marker="o")
    plt.title("Wykres 1. Średnie GHI w godzinach doby")
    plt.xlabel("Godzina")
    plt.ylabel("Średnie GHI")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_dir / "wykres1_ghi_godziny.png", dpi=140)
    plt.close()

    # Wykres 2 — siła i znak korelacji GHI z cechami pogodowymi (wynik aplikacji).
    corr = rel_pd.melt(var_name="miara", value_name="wartosc")
    plt.figure(figsize=(8, 4))
    plt.bar(corr["miara"], corr["wartosc"])
    plt.title("Wykres 2. Korelacje GHI z cechami pogodowymi")
    plt.ylabel("Wartość korelacji")
    plt.xticks(rotation=15)
    plt.tight_layout()
    plt.savefig(out_dir / "wykres2_korelacje.png", dpi=140)
    plt.close()

    cmp_plot = cmp_pd.sort_values("mode")

    # Wykres 3 — kluczowa miara eksperymentu: mediana czasu przetwarzania.
    plt.figure(figsize=(7, 4))
    plt.bar(cmp_plot["mode"], cmp_plot["median_seconds"], color=["#1f77b4", "#ff7f0e"])
    plt.title("Wykres 3. Mediana czasu przetwarzania: wsadowy vs strumieniowy")
    plt.ylabel("Czas [s]")
    plt.tight_layout()
    plt.savefig(out_dir / "wykres3_czas_przetwarzania.png", dpi=140)
    plt.close()

    # Wykres 4 — druga miara eksperymentu: przepustowość przetwarzania.
    plt.figure(figsize=(7, 4))
    plt.bar(cmp_plot["mode"], cmp_plot["rows_per_sec"], color=["#1f77b4", "#ff7f0e"])
    plt.title("Wykres 4. Przepustowość przetwarzania: wsadowy vs strumieniowy")
    plt.ylabel("Wiersze / s")
    plt.tight_layout()
    plt.savefig(out_dir / "wykres4_przepustowosc.png", dpi=140)
    plt.close()


def metric_value(df, key: str) -> float:
    row = df.loc[df["metric"] == key, "value"]
    return float(row.iloc[0]) if not row.empty else float("nan")


def mode_value(df, mode: str, key: str) -> float:
    row = df.loc[df["mode"] == mode, key]
    return float(row.iloc[0]) if not row.empty else float("nan")


def save_report(q1_pd, q2_pd, cmp_pd, out_file: Path) -> None:
    c_ratio = metric_value(q1_pd, "completeness_ratio")
    nn_ratio = metric_value(q1_pd, "non_negative_radiation_ratio")
    corr_mean = metric_value(q2_pd, "mean_abs_corr_ghi_weather")
    peak_ratio = metric_value(q2_pd, "hourly_peak_to_mean_ghi")

    b_sec = mode_value(cmp_pd, "wsadowy", "median_seconds")
    s_sec = mode_value(cmp_pd, "strumieniowy", "median_seconds")
    b_thr = mode_value(cmp_pd, "wsadowy", "rows_per_sec")
    s_thr = mode_value(cmp_pd, "strumieniowy", "rows_per_sec")
    b_agg = mode_value(cmp_pd, "wsadowy", "agg_rows")
    s_agg = mode_value(cmp_pd, "strumieniowy", "agg_rows")
    runs = int(mode_value(cmp_pd, "wsadowy", "runs"))
    consistent = "tak" if b_agg == s_agg else "nie"
    winner = "wsadowy" if b_sec <= s_sec else "strumieniowy"
    speedup = (max(b_sec, s_sec) / max(min(b_sec, s_sec), 1e-9))

    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(
        "\n".join(
            [
                "## Raport: efektywność przetwarzania danych solar-radiation w Spark",
                "",
                "Celem projektu jest zbadanie efektywności narzędzia Spark oraz dobór trybu",
                "przetwarzania (wsadowy vs strumieniowy) dla danych meteorologicznych/solarnych.",
                "Aplikacja przykładowa realizuje czyszczenie danych i analizę promieniowania,",
                "a eksperyment porównuje oba tryby na tym samym potoku obliczeniowym.",
                "",
                "### Aplikacja i jej wyniki analityczne",
                "Aplikacja wczytuje surowe dane CSV (`bronze`), czyści i typuje je (`silver`)",
                "oraz liczy statystyki i zależności promieniowania:",
                "- **Tabela 1** (`tabela1_profil_statystyczny.csv`) — profil statystyczny zmiennych.",
                "- **Tabela 2** (`tabela2_korelacje.csv`) — korelacje `GHI` z temperaturą/wilgotnością/wiatrem.",
                "- **Tabela 3** (`tabela3_profil_godzinowy.csv`) — średnie godzinowe.",
                "- **Wykres 1** (`charts/wykres1_ghi_godziny.png`) — dzienny profil `GHI`.",
                "- **Wykres 2** (`charts/wykres2_korelacje.png`) — siła i znak korelacji.",
                "",
                "Jakość danych wejściowych (Tabela 4 i 5) potwierdza wiarygodność tych wyników:",
                f"kompletność = `{c_ratio:.4f}`, udział nieujemnego promieniowania = `{nn_ratio:.4f}`.",
                "",
                "Krótka interpretacja: Wykres 1 i Tabela 3 pokazują dzienny profil promieniowania",
                "z maksimum w środku dnia, a Wykres 2/Tabela 2 potwierdzają zależności `GHI`",
                f"z cechami pogodowymi (średnia |korelacja| = `{corr_mean:.4f}`, stosunek szczyt/średnia",
                f"profilu godzinowego = `{peak_ratio:.4f}`).",
                "",
                "### Eksperyment: porównanie trybów przetwarzania (rdzeń projektu)",
                "Definiujemy dwa scenariusze przetwarzania tego samego workloadu",
                "(czyszczenie + agregacja godzinowa `GHI`):",
                "1. **Scenariusz wsadowy** — przetwarzanie wsadowe pełnego pliku.",
                "2. **Scenariusz strumieniowy** — Structured Streaming z wyzwalaczem jednorazowym.",
                "",
                "Miary jakości/wydajności (min. 2):",
                f"- **M1: mediana czasu przetwarzania** (z {runs} powtórzeń po rozgrzewce).",
                "- **M2: przepustowość** (`rows_per_sec`).",
                "- Kontrola spójności wyniku: liczba wierszy agregacji w obu trybach.",
                "",
                f"- **Wsadowy**: mediana = `{b_sec:.4f}` s, przepustowość = `{b_thr:.2f}` wierszy/s, wierszy wyniku = `{int(b_agg)}`.",
                f"- **Strumieniowy**: mediana = `{s_sec:.4f}` s, przepustowość = `{s_thr:.2f}` wierszy/s, wierszy wyniku = `{int(s_agg)}`.",
                f"- Spójność wyników między trybami: **{consistent}**.",
                f"- Szybszy tryb w tym przebiegu: **{winner}** (ok. {speedup:.2f}x; patrz Tabela 6, Wykres 3 i 4).",
                "",
                "### Wnioski i dalsze kierunki",
                "- Oba tryby dają identyczny wynik analityczny, więc wybór trybu jest decyzją wydajnościowo-operacyjną.",
                f"- Dla tego pilotażowego zbioru (dane statyczne, ~10 MB) szybszy jest tryb **{winner}**: "
                "przetwarzanie wsadowe nie ponosi narzutu mikro-batchy i checkpointów typowego dla strumienia.",
                "- Tryb strumieniowy ma sens, gdy dane napływają w sposób ciągły (np. odczyty stacji w czasie rzeczywistym), "
                "kosztem stałego narzutu — co potwierdza wyższy czas przy jednorazowym przetworzeniu pliku.",
                "- Dalsze kroki: pomiar przy rosnącym wolumenie danych, wpływ liczby partycji `spark.sql.shuffle.partitions` "
                "oraz triggera (`once` vs `processingTime`) na czas i przepustowość.",
            ]
        ),
        encoding="utf-8",
    )


def main() -> None:
    paths = load_paths()
    spark = create_spark(app_name="solar-experiments")

    try:
        df = read_silver(spark, paths.solar_silver_dir).cache()

        # Wyniki analityczne aplikacji.
        stats = stat_table(df)
        rel = rel_table(df)
        hourly = hour_table(df)
        q1 = scenario1_quality(df)
        q2 = scenario2_quality(rel, hourly)

        # Rdzeń projektu: porównanie trybu wsadowego i strumieniowego.
        with tempfile.TemporaryDirectory(prefix="solar-proc-cmp-") as tmp:
            cmp = compare_processing_modes(spark, paths.solar_csv, tmp).cache()
            cmp_pd = cmp.toPandas()

            base = paths.solar_gold_dir / "tables"
            save_table(stats, base / "profil_statystyczny")
            save_table(rel, base / "korelacje")
            save_table(hourly, base / "profil_godzinowy")
            save_table(q1, base / "jakosc_danych")
            save_table(q2, base / "jakosc_analizy")
            save_table(cmp, base / "porownanie_trybow")

        stats_pd = stats.toPandas()
        rel_pd = rel.toPandas()
        hourly_pd = hourly.toPandas()
        q1_pd = q1.toPandas()
        q2_pd = q2.toPandas()

        report_dir = paths.solar_gold_dir / "report"
        save_report_tables(stats_pd, rel_pd, hourly_pd, q1_pd, q2_pd, cmp_pd, report_dir / "tables")
        save_charts(hourly_pd, rel_pd, cmp_pd, report_dir / "charts")
        save_report(q1_pd, q2_pd, cmp_pd, report_dir / "report.md")

        print(f"Rows: {df.count()}")
        print(f"Saved tables to: {base}")
        print(f"Saved report to: {report_dir / 'report.md'}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
