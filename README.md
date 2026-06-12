## Spark Meteo Analysis

### Dane
- `data/bronze/solar-radiation/2017_2019.csv`
- `data/silver/solar-radiation/prepared`
- tabele eksperymentów: `data/gold/solar-radiation/tables`
- artefakty raportu (tabele CSV, wykresy, opis): `data/gold/solar-radiation/report`

### Uruchomienie
```bash
python -m spark_radiation_analysis.jobs.prepare_solar_radiation
python -m spark_radiation_analysis.jobs.solar_experiments
python -m unittest discover -s tests -v
```

### Cel projektu
Zbadanie efektywności Sparka i dobór trybu przetwarzania (wsadowy vs strumieniowy)
dla danych solarnych. Aplikacja przykładowa liczy analitykę promieniowania,
a eksperyment porównuje oba tryby na tym samym potoku obliczeniowym.

### Wyniki analityczne aplikacji (solar-radiation)
- `profil_statystyczny`: profil statystyczny (średnia, odchylenie, min/max, liczność)
- `korelacje`: korelacje `GHI` z temperaturą, wilgotnością i wiatrem
- `profil_godzinowy`: wzorce godzinowe (`avg_ghi`, `avg_temp`, `avg_rh`, `avg_wind`)

### Eksperyment: porównanie trybów przetwarzania (rdzeń projektu)
- `porownanie_trybow`: ten sam workload (czyszczenie + agregacja godzinowa `GHI`)
  uruchomiony `wsadowo` i `strumieniowo`; mierzone metryki wydajności:
  `median_seconds`, `min_seconds`, `rows_per_sec` oraz `agg_rows` (kontrola spójności wyniku)
- pomiar: rozgrzewka + powtórzenia (mediana) ograniczające narzut inicjalizacji Sparka

### Miary jakości
- jakość danych: `completeness_ratio`, `non_negative_radiation_ratio`
- jakość analizy: `mean_abs_corr_ghi_weather`, `hourly_peak_to_mean_ghi`

### Testy
- testy z małymi, jawnie wpisanymi danymi weryfikują deterministycznie logikę transformacji (to testy poprawności, nie benchmark)
- test integracyjny `test_prepare_solar_from_project_bronze` sprawdza pipeline na realnym pliku `data/bronze/solar-radiation/2017_2019.csv`
- test `test_quality_metrics_on_project_silver` waliduje miary jakości na realnych danych `silver`
- test `test_compare_modes_metrics_and_consistency` sprawdza eksperyment `wsadowy` vs `strumieniowy`: metryki wydajności oraz spójność wyniku agregacji między trybami
- test E2E `test_solar_experiments_outputs_and_metrics` sprawdza artefakty wymagane zadaniem: wyniki aplikacji, miary jakości, eksperyment trybów, tabele, wykresy i raport