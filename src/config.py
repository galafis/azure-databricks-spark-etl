"""Configuracoes centralizadas do pipeline Spark ETL."""

import os
from dataclasses import dataclass, field
from typing import Dict, Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass
class SparkConfig:
    """Configuracoes do Apache Spark."""

    app_name: str = "AzureDatabricksSparkETL"
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    log_level: str = os.getenv("SPARK_LOG_LEVEL", "WARN")
    shuffle_partitions: int = int(
        os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")
    )
    delta_extensions: str = (
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    delta_catalog: str = (
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )


@dataclass
class StorageConfig:
    """Configuracoes de armazenamento."""

    bronze_path: str = os.getenv(
        "BRONZE_PATH", "data/delta/bronze"
    )
    silver_path: str = os.getenv(
        "SILVER_PATH", "data/delta/silver"
    )
    gold_path: str = os.getenv(
        "GOLD_PATH", "data/delta/gold"
    )
    input_path: str = os.getenv(
        "INPUT_PATH", "data/sample"
    )
    checkpoint_path: str = os.getenv(
        "CHECKPOINT_PATH", "data/checkpoints"
    )


@dataclass
class MLflowConfig:
    """Configuracoes do MLflow."""

    tracking_uri: str = os.getenv(
        "MLFLOW_TRACKING_URI", "mlruns"
    )
    experiment_name: str = os.getenv(
        "MLFLOW_EXPERIMENT", "spark-etl-pipeline"
    )


@dataclass
class PipelineSettings:
    """Configuracoes gerais do pipeline."""

    spark: SparkConfig = field(default_factory=SparkConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    mlflow: MLflowConfig = field(default_factory=MLflowConfig)
    batch_size: int = int(os.getenv("BATCH_SIZE", "10000"))
    enable_streaming: bool = (
        os.getenv("ENABLE_STREAMING", "false").lower() == "true"
    )
    environment: str = os.getenv("ENVIRONMENT", "development")
