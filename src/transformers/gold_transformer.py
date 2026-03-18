"""Transformador da camada Gold - dados agregados para analytics."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class GoldTransformer:
    """Agrega e prepara dados para consumo analitico."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def transform(self, silver_df: DataFrame) -> DataFrame:
        """Cria visao agregada por departamento."""
        logger.info("Iniciando transformacao Gold")

        result = silver_df.groupBy(
            "department_id", "department_name"
        ).agg(
            F.count("*").alias("total_employees"),
            F.round(F.avg("salary"), 2).alias("avg_salary"),
            F.round(F.min("salary"), 2).alias("min_salary"),
            F.round(F.max("salary"), 2).alias("max_salary"),
            F.round(F.stddev("salary"), 2).alias("stddev_salary"),
            F.round(F.avg("score"), 2).alias("avg_performance"),
            F.sum(
                F.when(F.col("score") >= 4.0, 1).otherwise(0)
            ).alias("high_performers"),
        ).withColumn(
            "salary_range",
            F.col("max_salary") - F.col("min_salary"),
        ).withColumn(
            "high_performer_pct",
            F.round(
                F.col("high_performers") / F.col("total_employees") * 100,
                1,
            ),
        ).withColumn(
            "_gold_timestamp", F.current_timestamp()
        ).orderBy(F.desc("total_employees"))

        logger.info(
            "Transformacao Gold concluida: %d departamentos",
            result.count(),
        )
        return result
