from __future__ import annotations

import shutil
import statistics
import uuid
from pathlib import Path
from time import perf_counter

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_radiation_analysis.ingestion.solar_radiation import (
    clean_solar,
    read_solar_csv,
    solar_schema,
)

# Reprezentatywny workload analityczny używany w obu trybach przetwarzania:
# czyszczenie + agregacja średniego GHI w przekroju godzin doby.
# Ten sam wynik liczymy wsadowo i strumieniowo, dzięki czemu porównanie
# dotyczy realnego przetwarzania danych, a nie tylko samego wczytania pliku.
def _hourly_ghi(df: DataFrame) -> DataFrame:
    return clean_solar(df).groupBy("Hour").agg(F.avg("GHI").alias("avg_ghi"))


def _input_rows(spark: SparkSession, csv_path: str | Path) -> int:
    # Liczba poprawnych rekordów wejściowych (po czyszczeniu) — podstawa przepustowości.
    return clean_solar(read_solar_csv(spark, csv_path)).count()


def _batch_once(spark: SparkSession, csv_path: str | Path) -> float:
    # Tryb wsadowy: pełny potok (wczytanie -> czyszczenie -> agregacja) i akcja count().
    result = _hourly_ghi(read_solar_csv(spark, csv_path))
    t0 = perf_counter()
    result.count()
    return perf_counter() - t0


def _stream_once(spark: SparkSession, src_dir: Path, chk_base: Path) -> tuple[float, int]:
    # Tryb strumieniowy: ten sam potok uruchamiany przez Structured Streaming
    # z wyzwalaczem jednorazowym (trigger once) i zapisem do pamięci.
    q_name = f"hourly_ghi_{uuid.uuid4().hex}"
    chk_dir = chk_base / uuid.uuid4().hex

    stream_df = (
        spark.readStream.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .option("enforceSchema", "true")
        .schema(solar_schema())
        .csv(str(src_dir))
    )
    result = _hourly_ghi(stream_df)

    t0 = perf_counter()
    query = (
        result.writeStream.format("memory")
        .queryName(q_name)
        .outputMode("complete")
        .option("checkpointLocation", str(chk_dir))
        .trigger(once=True)
        .start()
    )
    query.awaitTermination()
    seconds = perf_counter() - t0

    agg_rows = spark.table(q_name).count()
    spark.catalog.dropTempView(q_name)
    return seconds, agg_rows


def _summarize(mode: str, durations: list[float], input_rows: int, agg_rows: int) -> dict:
    median = statistics.median(durations)
    return {
        "mode": mode,
        "runs": len(durations),
        "input_rows": int(input_rows),
        "agg_rows": int(agg_rows),
        "median_seconds": float(median),
        "min_seconds": float(min(durations)),
        "rows_per_sec": float(input_rows) / max(median, 1e-9),
    }


def compare_processing_modes(
    spark: SparkSession,
    csv_path: str | Path,
    tmp_dir: str | Path,
    repeats: int = 3,
) -> DataFrame:
    """Porównuje wydajność trybu wsadowego i strumieniowego na tym samym workloadzie.

    Wejście: ścieżka do pliku CSV (warstwa bronze) oraz katalog roboczy.
    Wyjście: DataFrame z miarami wydajności dla obu trybów (mediana czasu,
    minimalny czas, przepustowość) oraz liczbą wierszy wyniku do kontroli spójności.

    Każdy tryb jest rozgrzewany jednym przebiegiem (odrzucanym), a następnie
    mierzony `repeats` razy, co ogranicza wpływ inicjalizacji JVM/Spark na wynik.
    """
    tmp = Path(tmp_dir)
    src_dir = tmp / "src"
    chk_base = tmp / "checkpoints"
    src_dir.mkdir(parents=True, exist_ok=True)
    chk_base.mkdir(parents=True, exist_ok=True)

    src = Path(csv_path)
    shutil.copy2(src, src_dir / src.name)

    input_rows = _input_rows(spark, csv_path)

    # Tryb wsadowy: rozgrzewka + pomiary.
    _batch_once(spark, csv_path)
    batch_durations = [_batch_once(spark, csv_path) for _ in range(repeats)]
    batch_agg = _hourly_ghi(read_solar_csv(spark, csv_path)).count()

    # Tryb strumieniowy: rozgrzewka + pomiary.
    _stream_once(spark, src_dir, chk_base)
    stream_durations: list[float] = []
    stream_agg = 0
    for _ in range(repeats):
        seconds, stream_agg = _stream_once(spark, src_dir, chk_base)
        stream_durations.append(seconds)

    rows = [
        _summarize("wsadowy", batch_durations, input_rows, batch_agg),
        _summarize("strumieniowy", stream_durations, input_rows, stream_agg),
    ]
    return spark.createDataFrame(rows)
