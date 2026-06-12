# Spark Meteo Analysis

Krótki projekt pokazujący użycie PySpark do wsadowego przetwarzania danych o promieniowaniu słonecznym i pogodzie.

## Struktura

```text
spark-meteo-analysis/
  solar_pipeline.ipynb
  data/
    bronze/
    silver/
  results/
    tables/
    plots/
  spark_radiation_analysis/
    __init__.py
    config.py
    spark_session.py
    schema.py
    processing.py
    experiments.py
    plotting.py
  requirements.txt
```

## Instalacja

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Wymagane: Python 3.10+ oraz lokalna Java dla PySpark.

## Uruchomienie

1. Umieść dane w `data/bronze/solar-radiation/2017_2019.csv`.
2. Uruchom notebook:

```bash
jupyter notebook solar_pipeline.ipynb
```

## Warstwa silver

Notebook przygotowuje oczyszczone dane i zapisuje je do:
`data/silver/solar_radiation_prepared/`

To jest warstwa po czyszczeniu i dodaniu kolumny `timestamp`, zapisana jako Parquet.
Ułatwia szybsze kolejne analizy i porównanie wydajności `bronze` vs `silver`.

## Co robi notebook

- tworzy `SparkSession`,
- wczytuje CSV z `data/bronze/`,
- przygotowuje kolumnę `timestamp`,
- zapisuje prepared data do `data/silver/`,
- uruchamia eksperyment baseline i optimized,
- porównuje czas operacji dla `bronze` i `silver`,
- zapisuje tabele do `results/tables/`,
- zapisuje wykresy do `results/plots/`,
- pokazuje 2 krótkie case'y badawcze: GHI i Relative Humidity.
