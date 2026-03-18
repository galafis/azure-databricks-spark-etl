"""Carregador de dados em formato Delta Lake."""

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.config import StorageConfig

logger = logging.getLogger(__name__)


class DeltaLoader:
    """Persiste DataFrames em formato Delta Lake."""

    def __init__(
        self, spark: SparkSession, storage: StorageConfig
    ) -> None:
        self.spark = spark
        self.storage = storage

    def save_to_delta(
        self,
        df: DataFrame,
        layer: str,
        table_name: str,
        mode: str = "overwrite",
        partition_by: list = None,
    ) -> None:
        """Salva DataFrame em Delta Lake na camada especificada."""
        layer_paths = {
            "bronze": self.storage.bronze_path,
            "silver": self.storage.silver_path,
            "gold": self.storage.gold_path,
        }
        base_path = layer_paths.get(layer)
        if not base_path:
            raise ValueError(f"Camada invalida: {layer}")

        output_path = str(Path(base_path) / table_name)
        logger.info(
            "Salvando %d registros em Delta: %s",
            df.count(),
            output_path,
        )

        writer = df.write.format("delta").mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(output_path)
        logger.info("Dados salvos com sucesso em %s", output_path)

    def read_delta(
        self, layer: str, table_name: str
    ) -> DataFrame:
        """Le dados de uma tabela Delta Lake."""
        layer_paths = {
            "bronze": self.storage.bronze_path,
            "silver": self.storage.silver_path,
            "gold": self.storage.gold_path,
        }
        base_path = layer_paths.get(layer)
        if not base_path:
            raise ValueError(f"Camada invalida: {layer}")

        table_path = str(Path(base_path) / table_name)
        return self.spark.read.format("delta").load(table_path)
