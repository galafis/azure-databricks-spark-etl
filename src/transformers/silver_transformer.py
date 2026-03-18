"""Transformador da camada Silver - dados limpos e validados."""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class SilverTransformer:
    """Limpa, valida e enriquece dados para a camada Silver."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def transform(
        self, bronze_df: DataFrame, dept_df: DataFrame
    ) -> DataFrame:
        """Aplica transformacoes Silver ao DataFrame."""
        logger.info("Iniciando transformacao Silver")

        result = bronze_df

        # Remover duplicatas
        result = self._remove_duplicates(result)

        # Tratar valores nulos
        result = self._handle_nulls(result)

        # Validar tipos e formatos
        result = self._validate_types(result)

        # Enriquecer com dados de departamento
        result = self._join_departments(result, dept_df)

        # Adicionar colunas derivadas
        result = self._add_derived_columns(result)

        logger.info(
            "Transformacao Silver concluida: %d registros",
            result.count(),
        )
        return result

    def _remove_duplicates(
        self, df: DataFrame
    ) -> DataFrame:
        """Remove registros duplicados."""
        before = df.count()
        result = df.dropDuplicates()
        after = result.count()
        logger.info(
            "Duplicatas removidas: %d -> %d", before, after
        )
        return result

    def _handle_nulls(
        self, df: DataFrame
    ) -> DataFrame:
        """Trata valores nulos de forma inteligente."""
        numeric_cols = [
            f.name for f in df.schema.fields
            if str(f.dataType) in ("DoubleType()", "IntegerType()")
        ]
        for col_name in numeric_cols:
            median_val = df.approxQuantile(
                col_name, [0.5], 0.01
            )
            if median_val:
                df = df.fillna(
                    {col_name: median_val[0]}
                )

        string_cols = [
            f.name for f in df.schema.fields
            if str(f.dataType) == "StringType()"
        ]
        for col_name in string_cols:
            df = df.fillna({col_name: "N/A"})

        return df

    def _validate_types(
        self, df: DataFrame
    ) -> DataFrame:
        """Valida e converte tipos de dados."""
        if "salary" in df.columns:
            df = df.withColumn(
                "salary",
                F.col("salary").cast("double"),
            )
        if "hire_date" in df.columns:
            df = df.withColumn(
                "hire_date",
                F.to_date(F.col("hire_date")),
            )
        return df

    def _join_departments(
        self, df: DataFrame, dept_df: DataFrame
    ) -> DataFrame:
        """Enriquece com informacoes de departamento."""
        if "department_id" in df.columns:
            dept_clean = dept_df.select(
                F.col("department_id"),
                F.col("name").alias("department_name"),
                F.col("manager").alias("department_manager"),
            )
            df = df.join(
                dept_clean, on="department_id", how="left"
            )
        return df

    def _add_derived_columns(
        self, df: DataFrame
    ) -> DataFrame:
        """Adiciona colunas calculadas."""
        if "salary" in df.columns:
            w = Window.partitionBy("department_id")
            df = df.withColumn(
                "salary_rank",
                F.dense_rank().over(w.orderBy(F.desc("salary"))),
            ).withColumn(
                "dept_avg_salary",
                F.avg("salary").over(w),
            )
        return df
