"""Extrator de dados em lote usando Apache Spark."""

import logging
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.config import StorageConfig

logger = logging.getLogger(__name__)


class BatchExtractor:
    """Extrai dados de diversas fontes em modo batch."""

    def __init__(
        self, spark: SparkSession, storage: StorageConfig
    ) -> None:
        self.spark = spark
        self.storage = storage

    def extract_csv(
        self,
        filename: str,
        schema: Optional[StructType] = None,
        header: bool = True,
        delimiter: str = ",",
    ) -> DataFrame:
        """Extrai dados de um arquivo CSV."""
        filepath = Path(self.storage.input_path) / filename
        logger.info("Extraindo CSV: %s", filepath)

        reader = self.spark.read.option(
            "header", str(header).lower()
        ).option("delimiter", delimiter)

        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", "true")

        df = reader.csv(str(filepath))
        logger.info(
            "CSV extraido: %d registros, %d colunas",
            df.count(),
            len(df.columns),
        )
        return df

    def extract_json(
        self,
        filename: str,
        multiline: bool = False,
    ) -> DataFrame:
        """Extrai dados de um arquivo JSON."""
        filepath = Path(self.storage.input_path) / filename
        logger.info("Extraindo JSON: %s", filepath)

        df = self.spark.read.option(
            "multiline", str(multiline).lower()
        ).json(str(filepath))

        logger.info(
            "JSON extraido: %d registros", df.count()
        )
        return df

    def extract_parquet(
        self, filename: str
    ) -> DataFrame:
        """Extrai dados de um arquivo Parquet."""
        filepath = Path(self.storage.input_path) / filename
        logger.info("Extraindo Parquet: %s", filepath)
        df = self.spark.read.parquet(str(filepath))
        logger.info(
            "Parquet extraido: %d registros", df.count()
        )
        return df

    def extract_delta(
        self, table_path: str
    ) -> DataFrame:
        """Extrai dados de uma tabela Delta Lake."""
        logger.info("Extraindo Delta: %s", table_path)
        df = self.spark.read.format("delta").load(table_path)
        logger.info(
            "Delta extraido: %d registros", df.count()
        )
        return df
