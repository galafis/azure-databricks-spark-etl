"""Factory para criacao de SparkSession configurada."""

import logging

from pyspark.sql import SparkSession

from src.config import SparkConfig

logger = logging.getLogger(__name__)


def create_spark_session(
    config: SparkConfig,
) -> SparkSession:
    """Cria e retorna uma SparkSession com Delta Lake."""
    logger.info(
        "Criando SparkSession: %s", config.app_name
    )

    builder = (
        SparkSession.builder.appName(config.app_name)
        .master(config.master)
        .config(
            "spark.sql.extensions",
            config.delta_extensions,
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            config.delta_catalog,
        )
        .config(
            "spark.sql.shuffle.partitions",
            str(config.shuffle_partitions),
        )
        .config(
            "spark.sql.adaptive.enabled", "true"
        )
        .config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer",
        )
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(config.log_level)
    logger.info("SparkSession criada com sucesso.")
    return spark
