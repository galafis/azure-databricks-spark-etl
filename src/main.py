"""Ponto de entrada principal do pipeline Spark ETL.

Orquestra o fluxo completo: extracao, transformacao (Bronze/Silver/Gold)
e carga em Delta Lake com rastreamento via MLflow.
"""

import logging
import sys
import time
from typing import Optional

from pyspark.sql import SparkSession

from src.config import PipelineSettings
from src.extractors.batch_extractor import BatchExtractor
from src.transformers.bronze_transformer import BronzeTransformer
from src.transformers.silver_transformer import SilverTransformer
from src.transformers.gold_transformer import GoldTransformer
from src.loaders.delta_loader import DeltaLoader
from src.utils.spark_session import create_spark_session
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def run_pipeline(
    settings: Optional[PipelineSettings] = None,
) -> None:
    """Executa o pipeline ETL completo."""
    if settings is None:
        settings = PipelineSettings()

    logger.info("Iniciando pipeline Spark ETL...")
    start_time = time.time()

    spark = create_spark_session(settings.spark)

    try:
        # Extracao
        logger.info("Etapa 1: Extracao de dados")
        extractor = BatchExtractor(spark, settings.storage)
        raw_df = extractor.extract_csv("employees.csv")
        dept_df = extractor.extract_csv("departments.csv")
        logger.info(
            "Registros extraidos: %d funcionarios, %d departamentos",
            raw_df.count(),
            dept_df.count(),
        )

        # Bronze
        logger.info("Etapa 2: Transformacao Bronze")
        bronze = BronzeTransformer(spark)
        bronze_df = bronze.transform(raw_df)

        # Silver
        logger.info("Etapa 3: Transformacao Silver")
        silver = SilverTransformer(spark)
        silver_df = silver.transform(bronze_df, dept_df)

        # Gold
        logger.info("Etapa 4: Transformacao Gold")
        gold = GoldTransformer(spark)
        gold_df = gold.transform(silver_df)

        # Carga em Delta Lake
        logger.info("Etapa 5: Carga em Delta Lake")
        loader = DeltaLoader(spark, settings.storage)
        loader.save_to_delta(bronze_df, "bronze", "employees")
        loader.save_to_delta(silver_df, "silver", "employees")
        loader.save_to_delta(gold_df, "gold", "department_metrics")

        duration = time.time() - start_time
        logger.info(
            "Pipeline concluido com sucesso em %.2f segundos.",
            duration,
        )

    except Exception as e:
        logger.error("Erro no pipeline: %s", str(e))
        raise
    finally:
        spark.stop()
        logger.info("SparkSession encerrada.")


if __name__ == "__main__":
    run_pipeline()
