"""Transformador da camada Bronze - dados brutos com metadados."""

import logging
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class BronzeTransformer:
    """Aplica transformacoes minimas para a camada Bronze."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def transform(self, df: DataFrame) -> DataFrame:
        """Adiciona metadados de ingestao e normaliza colunas."""
        logger.info(
            "Iniciando transformacao Bronze: %d registros",
            df.count(),
        )

        result = df

        # Normalizar nomes de colunas
        for col_name in result.columns:
            new_name = (
                col_name.strip()
                .lower()
                .replace(" ", "_")
                .replace("-", "_")
            )
            result = result.withColumnRenamed(col_name, new_name)

        # Adicionar metadados de ingestao
        result = result.withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        ).withColumn(
            "_source_file",
            F.input_file_name(),
        ).withColumn(
            "_ingestion_date",
            F.current_date(),
        )

        logger.info(
            "Transformacao Bronze concluida: %d colunas",
            len(result.columns),
        )
        return result
