# Architecture Documentation

## System Architecture

```mermaid
flowchart TB
    subgraph Sources["Data Sources"]
        S1[Azure Blob Storage]
        S2[Azure SQL Database]
        S3[REST APIs]
    end

    subgraph ETL["Databricks ETL Pipeline"]
        E[Extractors] --> T[Transformers]
        T --> V[Validators]
        V --> L[Loaders]
    end

    subgraph Storage["Delta Lake"]
        B[Bronze Layer] --> Si[Silver Layer]
        Si --> G[Gold Layer]
    end

    subgraph Monitoring["Observability"]
        ML[MLflow Tracking]
        LG[Structured Logging]
        MT[Metrics Dashboard]
    end

    Sources --> E
    L --> B
    ETL --> Monitoring
```

## Data Flow

### Bronze Layer (Raw)
- Raw data ingested without transformation
- Schema-on-read with full audit trail
- Partitioned by ingestion date

### Silver Layer (Cleaned)
- Data type enforcement and null handling
- Deduplication and constraint validation
- Business key standardization

### Gold Layer (Business)
- Aggregated metrics and KPIs
- Denormalized for analytics consumption
- Optimized for query performance (Z-ORDER)

## Component Design

```mermaid
classDiagram
    class PipelineConfig {
        +str source_path
        +str target_path
        +int batch_size
        +validate() bool
    }

    class DataExtractor {
        -PipelineConfig config
        +extract() DataFrame
        -_read_source() DataFrame
    }

    class DataTransformer {
        -PipelineConfig config
        +transform(df) DataFrame
        -_normalize_strings(df) DataFrame
        -_handle_nulls(df) DataFrame
        -_compute_features(df) DataFrame
    }

    class DataValidator {
        +validate(df) ValidationResult
        -_check_duplicates(df) List
        -_check_nulls(df) List
        -_check_ranges(df) List
    }

    class DeltaLoader {
        -PipelineConfig config
        +load(df, layer) None
        -_merge_into_delta(df) None
    }

    PipelineConfig <-- DataExtractor
    PipelineConfig <-- DataTransformer
    PipelineConfig <-- DeltaLoader
    DataExtractor --> DataTransformer
    DataTransformer --> DataValidator
    DataValidator --> DeltaLoader
```

## Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| Compute | Azure Databricks | Distributed processing |
| Processing | Apache Spark 3.5 / PySpark | Data transformations |
| Storage | Delta Lake | ACID transactions |
| Orchestration | Databricks Workflows | Job scheduling |
| Experiment Tracking | MLflow | Model and pipeline versioning |
| CI/CD | GitHub Actions | Automated testing and linting |
| Containerization | Docker | Reproducible environments |
| Language | Python 3.9+ | Core pipeline logic |

## Deployment Strategy

1. **Development**: Local Docker container with Spark standalone
2. **Staging**: Databricks workspace with test cluster
3. **Production**: Databricks with auto-scaling cluster, scheduled via Workflows

## Connection to HR Tech / People Analytics

This pipeline architecture directly supports HR Tech products such as **TOTVS RH People Analytics** by:

- Processing employee lifecycle data at scale (hiring, performance, turnover)
- Computing workforce KPIs (retention rate, salary equity, headcount forecasting)
- Enabling real-time dashboards for HR decision-makers
- Supporting compliance reporting (eSocial, RAIS) through standardized data models
- Providing the data foundation for predictive models (attrition risk, promotion readiness)
