from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def apply_clickstream_transform(df: DataFrame) -> DataFrame:
    out = df

    if "results_count" in out.columns:
        out = out.withColumn("results_count", col("results_count").cast("int"))

    return out