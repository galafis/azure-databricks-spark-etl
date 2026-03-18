# Data Directory

## Overview

This directory contains data assets used by the ETL pipeline. For security and compliance reasons, **no real sensitive data is stored in this repository**.

## Data Dictionary

| Column | Type | Description | Nullable | Example |
|---|---|---|---|---|
| employee_id | INTEGER | Unique employee identifier | No | 1001 |
| department | STRING | Department name | No | Engineering |
| salary | FLOAT | Annual salary in BRL | No | 85000.00 |
| hire_date | DATE | Employment start date | No | 2020-01-15 |
| performance_score | FLOAT | Annual review score (1.0-5.0) | Yes | 4.2 |
| is_active | BOOLEAN | Current employment status | No | True |
| tenure_years | FLOAT | Years since hire (computed) | No | 4.1 |
| salary_band | STRING | Salary classification (computed) | No | Senior |

## Synthetic Data Generation

Run the following to generate sample data:

```bash
python -m src.generate_data --rows 10000 --output data/sample_employees.parquet
```

## Public Data Sources Referenced

- **RAIS (Relacao Anual de Informacoes Sociais)**: Brazilian Ministry of Labor employment statistics
  - URL: https://bi.mte.gov.br/bgcaged/
  - Used for: Salary distribution benchmarks by sector
- **IBGE PNAD**: National household survey for demographic distributions
  - URL: https://www.ibge.gov.br/estatisticas/sociais/trabalho.html
  - Used for: Regional employment patterns

## Important Notes

- All data in this directory is **synthetic** and generated for demonstration purposes
- No personally identifiable information (PII) is present
- Data distributions are modeled after publicly available Brazilian labor statistics
- For production use, configure your own data sources via `.env` file
