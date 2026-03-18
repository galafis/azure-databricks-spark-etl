"""Unit tests for the Azure Databricks Spark ETL pipeline.

Tests cover data extraction, transformation, validation,
and quality checks across the pipeline stages.
"""

import os
import sys
import unittest
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config import PipelineConfig
from src.extractor import DataExtractor
from src.transformer import DataTransformer
from src.validator import DataValidator
from src.logger import setup_logger

logger = setup_logger(__name__)


class TestPipelineConfig(unittest.TestCase):
    """Tests for pipeline configuration loading and validation."""

    def test_config_loads_defaults(self) -> None:
        """Verify default configuration values are set correctly."""
        config = PipelineConfig()
        self.assertIsNotNone(config.source_path)
        self.assertIsNotNone(config.target_path)
        self.assertGreater(config.batch_size, 0)

    def test_config_validates_required_fields(self) -> None:
        """Ensure missing required fields raise ValueError."""
        with self.assertRaises(ValueError):
            PipelineConfig(source_path="", target_path="")

    def test_config_env_override(self) -> None:
        """Check that environment variables override defaults."""
        with patch.dict(os.environ, {"ETL_BATCH_SIZE": "5000"}):
            config = PipelineConfig()
            self.assertEqual(config.batch_size, 5000)


class TestDataExtractor(unittest.TestCase):
    """Tests for data extraction from various sources."""

    def setUp(self) -> None:
        """Set up test fixtures with sample data."""
        self.config = PipelineConfig()
        self.extractor = DataExtractor(self.config)
        self.sample_data = pd.DataFrame({
            "employee_id": [1001, 1002, 1003, 1004, 1005],
            "department": ["Engineering", "HR", "Finance", "Engineering", "HR"],
            "salary": [85000.0, 72000.0, 91000.0, 78000.0, 68000.0],
            "hire_date": pd.to_datetime([
                "2020-01-15", "2019-06-01", "2021-03-20",
                "2022-07-10", "2018-11-30"
            ]),
            "performance_score": [4.2, 3.8, 4.5, 3.9, 4.1],
            "is_active": [True, True, True, False, True],
        })

    def test_extract_returns_dataframe(self) -> None:
        """Extraction must return a pandas DataFrame."""
        with patch.object(self.extractor, "_read_source", return_value=self.sample_data):
            result = self.extractor.extract()
            self.assertIsInstance(result, pd.DataFrame)

    def test_extract_preserves_row_count(self) -> None:
        """Extracted data should not lose rows."""
        with patch.object(self.extractor, "_read_source", return_value=self.sample_data):
            result = self.extractor.extract()
            self.assertEqual(len(result), 5)

    def test_extract_schema_validation(self) -> None:
        """Verify extracted schema matches expected columns."""
        expected_cols = {"employee_id", "department", "salary", "hire_date", "performance_score", "is_active"}
        with patch.object(self.extractor, "_read_source", return_value=self.sample_data):
            result = self.extractor.extract()
            self.assertEqual(set(result.columns), expected_cols)


class TestDataTransformer(unittest.TestCase):
    """Tests for data transformation logic."""

    def setUp(self) -> None:
        """Set up transformer with sample data."""
        self.config = PipelineConfig()
        self.transformer = DataTransformer(self.config)
        self.raw_data = pd.DataFrame({
            "employee_id": [1001, 1002, 1003],
            "department": ["  Engineering ", "hr", "FINANCE"],
            "salary": [85000.0, None, 91000.0],
            "hire_date": pd.to_datetime(["2020-01-15", "2019-06-01", "2021-03-20"]),
            "performance_score": [4.2, 3.8, None],
            "is_active": [True, True, False],
        })

    def test_normalize_department_names(self) -> None:
        """Department names must be title-case and stripped."""
        result = self.transformer.transform(self.raw_data)
        expected = ["Engineering", "Hr", "Finance"]
        self.assertEqual(result["department"].tolist(), expected)

    def test_null_handling_salary(self) -> None:
        """Null salaries should be filled with department median."""
        result = self.transformer.transform(self.raw_data)
        self.assertFalse(result["salary"].isnull().any())

    def test_null_handling_performance(self) -> None:
        """Null performance scores should be filled with 0.0."""
        result = self.transformer.transform(self.raw_data)
        self.assertFalse(result["performance_score"].isnull().any())

    def test_tenure_calculation(self) -> None:
        """Tenure in years should be computed from hire_date."""
        result = self.transformer.transform(self.raw_data)
        self.assertIn("tenure_years", result.columns)
        self.assertTrue((result["tenure_years"] >= 0).all())

    def test_salary_band_assignment(self) -> None:
        """Salary bands should be assigned based on ranges."""
        result = self.transformer.transform(self.raw_data)
        self.assertIn("salary_band", result.columns)
        valid_bands = {"Junior", "Mid", "Senior", "Staff", "Principal"}
        for band in result["salary_band"]:
            self.assertIn(band, valid_bands)


class TestDataValidator(unittest.TestCase):
    """Tests for data quality validation rules."""

    def setUp(self) -> None:
        """Set up validator with clean and dirty data."""
        self.validator = DataValidator()
        self.clean_data = pd.DataFrame({
            "employee_id": [1001, 1002, 1003],
            "department": ["Engineering", "HR", "Finance"],
            "salary": [85000.0, 72000.0, 91000.0],
            "performance_score": [4.2, 3.8, 4.5],
        })
        self.dirty_data = pd.DataFrame({
            "employee_id": [1001, 1001, 1003],
            "department": ["Engineering", None, "Finance"],
            "salary": [-5000.0, 72000.0, 91000.0],
            "performance_score": [4.2, 6.0, 4.5],
        })

    def test_clean_data_passes_validation(self) -> None:
        """Clean data should pass all validation rules."""
        result = self.validator.validate(self.clean_data)
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.errors), 0)

    def test_duplicate_ids_detected(self) -> None:
        """Duplicate employee_id values must be flagged."""
        result = self.validator.validate(self.dirty_data)
        self.assertFalse(result.is_valid)
        self.assertTrue(any("duplicate" in e.lower() for e in result.errors))

    def test_negative_salary_detected(self) -> None:
        """Negative salary values must be flagged."""
        result = self.validator.validate(self.dirty_data)
        self.assertTrue(any("salary" in e.lower() for e in result.errors))

    def test_null_department_detected(self) -> None:
        """Null department values must be flagged."""
        result = self.validator.validate(self.dirty_data)
        self.assertTrue(any("null" in e.lower() or "missing" in e.lower() for e in result.errors))

    def test_performance_score_range(self) -> None:
        """Performance scores outside 0-5 must be flagged."""
        result = self.validator.validate(self.dirty_data)
        self.assertTrue(any("performance" in e.lower() or "range" in e.lower() for e in result.errors))


class TestEndToEndPipeline(unittest.TestCase):
    """Integration tests for the full ETL pipeline."""

    def test_pipeline_processes_sample_data(self) -> None:
        """Full pipeline should process sample data without errors."""
        sample = pd.DataFrame({
            "employee_id": range(1, 101),
            "department": np.random.choice(["Engineering", "HR", "Finance", "Marketing"], 100),
            "salary": np.random.uniform(50000, 150000, 100).round(2),
            "hire_date": pd.date_range("2018-01-01", periods=100, freq="W"),
            "performance_score": np.random.uniform(1.0, 5.0, 100).round(1),
            "is_active": np.random.choice([True, False], 100, p=[0.85, 0.15]),
        })
        config = PipelineConfig()
        transformer = DataTransformer(config)
        validator = DataValidator()

        transformed = transformer.transform(sample)
        validation = validator.validate(transformed)

        self.assertIsInstance(transformed, pd.DataFrame)
        self.assertEqual(len(transformed), 100)
        self.assertTrue(validation.is_valid)


if __name__ == "__main__":
    unittest.main()
