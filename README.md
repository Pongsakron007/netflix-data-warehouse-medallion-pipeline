# 🎬 Netflix Data Engineering Pipeline

> **A production-ready data pipeline implementing the Medallion Architecture (Bronze → Silver → Gold) for Netflix content analysis**

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)](https://databricks.com)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat&logo=delta&logoColor=white)](https://delta.io)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Pipeline Components](#pipeline-components)
- [Table Schema](#table-schema)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Testing Framework](#testing-framework)
- [Usage Examples](#usage-examples)
- [Performance Metrics](#performance-metrics)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## 🎯 Overview

This project implements a **scalable, production-ready data pipeline** for processing Netflix content data using Databricks and the **Medallion Architecture**. Built with **configurable dataclass-based pipeline classes**, the framework ingests raw data, applies comprehensive data quality validation, and transforms it into a **star schema** optimized for analytics and business intelligence.

### Key Features

✅ **Configurable Framework**: Dataclass-based `BronzeLayer`, `SilverLayer`, and `GoldLayer` classes  
✅ **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated) layers  
✅ **Databricks Auto Loader**: Incremental S3 ingestion with folder-path detection and schema evolution  
✅ **Data Quality Validation**: 8-stage quality check pipeline with bad record quarantine  
✅ **SCD Type 2**: Historical change tracking with temporal validity  
✅ **Star Schema**: 1 main dimension + 4 sub-dimensions + 4 bridge tables  
✅ **Hash-based Change Detection**: Efficient delta identification using SHA-256  
✅ **Incremental Processing**: Change Data Feed (CDF) enabled for all downstream layers  
✅ **Production Scalability**: Structured streaming with `trigger(availableNow=True)`  
✅ **Comprehensive Testing**: 5 automated test suites with 100% pass rate  
✅ **Production Performance**: 317+ records/second throughput  

### Business Use Cases

- 📊 **Content Analytics**: Analyze Netflix catalog trends, genres, and release patterns
- 🎭 **Talent Analysis**: Track actors, directors, and their collaboration networks
- 🌍 **Geographic Distribution**: Study content availability across countries
- 📈 **Time-series Analysis**: Monitor content additions and changes over time
- 🔍 **Data Quality Monitoring**: Track data integrity and validation metrics

---

## 🏗️ Architecture

### Medallion Architecture Pattern

```
                ┌─────────────────────────────────────────────────────────────────────┐
                │                         DATA FLOW PIPELINE                          │
                └─────────────────────────────────────────────────────────────────────┘

  📁 Source Files                🥉 Bronze Layer            🥈 Silver Layer               🥇 Gold Layer
  ─────────────                 ───────────────              ───────────────                ─────────────
       CSV                            │                            │                              │
       JSON          ──────►    Raw Data Store      ──────►   Star Schema        ──────►     Aggregations
      Parquet                   + Metadata                   + Quality Checks                 + Analytics
                                + CDF Enabled                + SCD Type 2                     + Metrics
                                + Auto Loader                + Normalization                  + BI-Ready

  ┌────────────┐              ┌────────────┐              ┌────────────────┐              ┌────────────┐
  │ netflix.csv│              │  netflix   │              │   dim_titles   │              │ Dashboards │
  │            │  ─────────►  │   _bronze  │  ─────────►  │ + 4 sub-dims   │  ─────────►  │    KPIs    │
  │ (17K rows) │              │            │              │ + 4 bridges    │              │  Reports   │
  └────────────┘              └────────────┘              │ + bad_records  │              └────────────┘
                                     ▲                     └────────────────┘
                                     │
                               config_table
                            (pipeline settings)
```

### Layer Responsibilities

#### 🥉 **Bronze Layer** - Raw Data Ingestion
- **Purpose**: Landing zone for external data with full audit trail
- **Class**: `BronzeLayer` (dataclass-based configuration)
- **Characteristics**: Immutable, append-only, schema-on-read
- **Key Features**:
  - **Databricks Auto Loader** for incremental S3/cloud storage ingestion
  - **Folder-path detection** - automatically discovers new files in directories
  - **Schema evolution** with rescue mode (`cloudFiles.schemaEvolutionMode: "rescue"`)
  - **Explicit schema definition** using StructType for type-safe metadata columns
  - **Checkpoint-based file tracking** - processes only new files
  - **Error handling** for Spark Connect serverless edge cases (SPARK-55448)
  - **Change Data Feed (CDF)** enabled for downstream incremental processing
  - File metadata tracking (`_load_dt`, `_file_name`, `_file_path`, `_file_size`, `_file_mod`)
  - Support for CSV, JSON, Parquet formats
  - Configurable via `config_table` or direct instantiation

**Auto Loader Modes**:
- **Batch Mode**: `read_from_file()` - one-time full load
- **Streaming Mode**: `s3_auto_loader()` - incremental with `trigger(availableNow=True)`

#### 🥈 **Silver Layer** - Data Quality & Normalization
- **Purpose**: Clean, validated, business-ready data
- **Class**: `SilverLayer` (dataclass-based configuration)
- **Characteristics**: Normalized, deduplicated, validated, SCD Type 2 enabled
- **Key Features**:

**8-Stage Data Quality Pipeline**:
1. `trim_data()` - Remove leading/trailing whitespace
2. `change_data_type()` - Cast to target types with `try_cast()` / `try_to_date()`
3. `get_invalid_record()` - Detect format violations via regex
4. `get_key_null_record()` - Identify null primary keys
5. `get_dup_record()` - Find row and key duplicates
6. `get_all_bad_record()` - Consolidate all bad records
7. `load_bad_record()` - Quarantine bad records with batch tracking
8. `get_final_result()` - Extract clean records

**Star Schema Transformation**:
- `get_hash_key_value()` - Generate SHA-256 hashes for CDC
- `load_sub_dimensions()` - Populate 4 dimension tables (cast, directors, countries, categories)
- `load_bridge_tables()` - Create 4 many-to-many relationship tables
- `load_main_dimension()` - Apply SCD Type 2 logic to main dimension
- `process_cdf_stream_to_silver()` - Orchestrate the entire incremental pipeline

**Output**: 9 tables (1 main dimension + 4 sub-dimensions + 4 bridges) + 1 bad record table

#### 🥇 **Gold Layer** - Business Aggregations
- **Purpose**: Optimized analytical tables for end-user consumption
- **Class**: `GoldLayer` (dataclass-based configuration)
- **Characteristics**: Denormalized, pre-aggregated, BI-ready
- **Key Features**:

**Business-Ready Tables**:
1. `create_gold_content_by_cast()` - Denormalized Title-Cast relationships
   - **Joins**: `dim_titles_silver` ⋈ `bridge_title_cast_silver` ⋈ `dim_cast_silver`
   - **Output**: One row per Title-Cast pair
   - **Business Question**: "Which actors appear in which titles?"

2. `create_gold_yearly_content_trends()` - Content volume by year and type
   - **Aggregation**: `GROUP BY release_year, type`
   - **Metrics**: Count of titles
   - **Business Question**: "How does content volume change by year and type?"

**Operational Model**:
- **Write Mode**: Always `overwrite` (full refresh)
- **Data Freshness**: Reflects latest Silver layer state
- **Query Performance**: Pre-joined and pre-aggregated for fast BI queries
- **Active Data Only**: Filters for `active_flag = True` from SCD Type 2 dimensions
- **Usage**: Dashboards, reports, ad-hoc analysis, self-service BI

---

## 📂 Project Structure

```
Databricks-for-Data-Engineers-Bootcamp2/
│
├── Netflix_project/
│   └── framework.ipynb                 # Main pipeline implementation
│       ├── BronzeLayer class          # Raw data ingestion logic
│       ├── SilverLayer class          # Data quality & transformation logic
│       ├── GoldLayer class            # Business aggregation logic
│       ├── Bronze Layer docs (MD)     # Step-by-step Bronze guide
│       ├── Silver Layer docs (MD)     # Step-by-step Silver guide
│       └── Gold Layer docs (MD)       # Step-by-step Gold guide
│
├── silver_layer_tests.py              # Comprehensive test suite
│   ├── SilverLayerTests class        # 5 automated test methods
│   └── StarSchemaQueries class       # SQL analytics helpers
│
├── README.md                          # This file (English)
├── README_TH.md                       # Thai documentation
│
└── Data Tables:
    ├── workspace.netflix.config_table                      # Pipeline configuration
    ├── workspace.netflix.netflix_bronze                    # Raw data (Bronze)
    ├── workspace.netflix.dim_titles_silver                 # Main dimension (Silver)
    ├── workspace.netflix.dim_cast_silver                   # Cast sub-dimension
    ├── workspace.netflix.dim_directors_silver              # Directors sub-dimension
    ├── workspace.netflix.dim_countries_silver              # Countries sub-dimension
    ├── workspace.netflix.dim_categories_silver             # Categories sub-dimension
    ├── workspace.netflix.bridge_title_cast_silver          # Title-Cast relationships
    ├── workspace.netflix.bridge_title_director_silver      # Title-Director relationships
    ├── workspace.netflix.bridge_title_country_silver       # Title-Country relationships
    ├── workspace.netflix.bridge_title_category_silver      # Title-Category relationships
    ├── workspace.netflix.netflix_bronze_bad_record         # Bad record quarantine
    ├── workspace.netflix.netflix_content_by_cast_gold      # Denormalized cast (Gold)
    └── workspace.netflix.netflix_yearly_content_trends_gold # Yearly trends (Gold)
```

---

## ⚙️ Pipeline Components

### 1. Configuration Management

**Table**: `workspace.netflix.config_table`

```python
# Centralized pipeline configuration
config_table columns:
- pipeline_name: str          # Unique identifier (e.g., "netflix")
- file_path: str              # Source data location (file or folder path)
- header: bool                # CSV header presence
- delimiter: str              # Field delimiter
- table_name: str             # Target Bronze table name
- schema_detail: map          # Column name → data type mapping
- keys: array                 # Primary key columns
- write_mode: str             # append/overwrite
```

### 2. Bronze Layer (`BronzeLayer` class)

**Factory Method**:
```python
bronze = BronzeLayer.from_config_table("netflix")
```

**Responsibilities**:
- Read data from files (CSV, JSON, Parquet) or folders
- Add metadata columns for source tracking
- Initialize Delta tables with CDF enabled
- Support both batch and streaming ingestion

**Key Methods**:
- `from_config_table(pipeline_name)` - Factory method from config
- `read_from_file()` - Batch mode: load file and add metadata
- `s3_auto_loader(checkpoint_location)` - Streaming mode: Auto Loader with checkpoint
- `load_to_bronze_table(df)` - Append data to Bronze table
- `_init_bronze_table()` - Initialize table with CDF on first run

**Auto Loader Configuration**:
```python
# Schema evolution and file discovery
.option("cloudFiles.schemaEvolutionMode", "rescue")
.option("pathGlobFilter", "*.csv")
.option("cloudFiles.schemaLocation", schema_location)
.option("mergeSchema", "true")
.trigger(availableNow=True)  # Batch-style streaming for cost efficiency
```

### 3. Silver Layer (`SilverLayer` class)

**Factory Method**:
```python
silver = SilverLayer.from_config_table("netflix")
```

**Responsibilities**:
- Process incremental changes via CDF
- Execute 8-stage data quality validation
- Transform to star schema (9 tables)
- Apply SCD Type 2 for historical tracking
- Quarantine bad records for audit

**Key Methods**:

#### Data Quality Pipeline:
1. `trim_data()` - Remove whitespace
2. `change_data_type()` - Cast data types
3. `get_invalid_record()` - Detect invalid values
4. `get_key_null_record()` - Find null keys
5. `get_dup_record()` - Identify duplicates (row and key)
6. `get_all_bad_record()` - Consolidate bad records
7. `load_bad_record()` - Audit trail
8. `get_final_result()` - Extract clean data

#### Star Schema Creation:
- `get_hash_key_value()` - Generate hashes for CDC
- `load_sub_dimensions()` - Populate masters (cast, directors, etc.)
- `load_bridge_tables()` - Create many-to-many relationships
- `load_main_dimension()` - SCD Type 2 upserts
- `process_cdf_stream_to_silver()` - Orchestrate full pipeline

**Incremental Processing**:
```python
# Read only changes from Bronze
bronze_cdf = (
    spark.readStream
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("workspace.netflix.netflix_bronze")
)
```

### 4. Gold Layer (`GoldLayer` class)

**Factory Method**:
```python
gold = GoldLayer.from_config_table("netflix")
```

**Responsibilities**:
- Create denormalized analytical tables
- Pre-compute business metrics and KPIs
- Filter for active data only (`active_flag = True`)
- Optimize for dashboard and BI tool consumption
- Full refresh pattern (overwrite mode)

**Key Methods**:

#### Denormalized Tables:
- `create_gold_content_by_cast()` - Flatten Title-Cast Many-to-Many relationships
  - Joins: `dim_titles_silver` ⋈ `bridge_title_cast_silver` ⋈ `dim_cast_silver`
  - Output: One row per Title-Cast pair
  - Business Question: "Which actors appear in which titles?"

- `create_gold_yearly_content_trends()` - Aggregate content volume by year and type
  - Aggregation: `GROUP BY release_year, type`
  - Metrics: Count of titles
  - Business Question: "How does content volume change by year and type?"

#### Pipeline Management:
- `from_config_table(pipeline_name)` - Factory method from config
- `run_gold_pipeline()` - Execute all Gold table creation methods

**Gold Table Characteristics**:
- **Write Mode**: Always `overwrite` (full refresh)
- **Data Freshness**: Reflects latest Silver layer state
- **Query Performance**: Pre-joined and pre-aggregated for speed
- **Usage**: Dashboards, reports, ad-hoc analysis, self-service BI

---

## 📊 Table Schema

### Main Dimension: `dim_titles_silver`

| Column | Type | Description |
|--------|------|-------------|
| `title_sk` | BIGINT | Surrogate key (auto-generated) |
| `show_id` | STRING | Business key from source |
| `type` | STRING | Movie or TV Show |
| `title` | STRING | Content title |
| `date_added` | DATE | Date added to Netflix |
| `release_year` | INT | Release year |
| `rating` | STRING | Content rating (PG, R, etc.) |
| `duration` | STRING | Runtime or number of seasons |
| `description` | STRING | Content synopsis |
| `hash_key` | STRING | SHA-256 of business keys |
| `hash_value` | STRING | SHA-256 of data columns |
| `active_flag` | BOOLEAN | Current version indicator |
| `start_date` | TIMESTAMP | Version effective from |
| `end_date` | TIMESTAMP | Version effective until (NULL = current) |
| `load_dt` | DATE | Load date |
| `load_dttm` | TIMESTAMP | Load timestamp |

### Sub-Dimensions

**dim_cast_silver** (36,399 actors):
- `cast_sk`, `cast_id`, `cast_name`

**dim_directors_silver** (4,996 directors):
- `director_sk`, `director_id`, `director_name`

**dim_countries_silver** (145 countries):
- `country_sk`, `country_id`, `country_name`

**dim_categories_silver** (73 categories):
- `category_sk`, `category_id`, `category_name`

### Bridge Tables (Many-to-Many Relationships)

**bridge_title_cast_silver** (128,818 relationships):
- `show_id`, `cast_id`

**bridge_title_director_silver** (14,039 relationships):
- `show_id`, `director_id`

**bridge_title_country_silver** (20,110 relationships):
- `show_id`, `country_id`

**bridge_title_category_silver** (38,848 relationships):
- `show_id`, `category_id`

### Audit Table: `netflix_bronze_bad_record`

| Column | Type | Description |
|--------|------|-------------|
| All source columns | Various | Original record |
| `reason` | ARRAY<STRING> | List of validation failures |
| `batch_id` | INT | Batch identifier |
| `load_dt` | DATE | Rejection date |
| `load_dttm` | TIMESTAMP | Rejection timestamp |

### Star Schema Diagram

```text

       [ Sub-Dimension: Directors ]                        [ Sub-Dimension: Cast ]
          dim_directors_silver                              dim_cast_silver
         ┌──────────────────────┐                         ┌──────────────────┐
         │ PK  │ director_sk    │                         │ PK  │ cast_sk    │
         │     │ director_id    │                         │     │ cast_id    │
         │     │ director_name  │                         │     │ cast_name  │
         └──────────┬───────────┘                         └────────┬─────────┘
                    │ (1)                                          │ (1)
                    ▼                                              ▼
                    ∞ (Many)                                       ∞ (Many)
       [ Bridge Table ]                                   [ Bridge Table ]
         bridge_title_director                             bridge_title_cast
         ┌──────────────────────┐                         ┌──────────────────┐
         │ FK  │ show_id        │                         │ FK  │ show_id    │
         │ FK  │ director_id    │                         │ FK  │ cast_id    │
         └──────────┬───────────┘                         └────────┬─────────┘
                    │                                              │
                    │                                              │
                    └───────────────┐              ┌───────────────┘
                            ∞ (Many)│              │ ∞ (Many)
                                    ▼              ▼
                        ┌────────────────────────────────────────┐
                        │         dim_titles_silver              │
                        │       (Main Fact Dimension)            │
                        ├────────────────────────────────────────┤
                        │ PK        │ title_sk                   │
                        │ BK        │ show_id                    │
                        │           │ type, title, date_added    │
                        │           │ release_year, rating       │
                        │           │ duration, description      │
                        │ SCD       │ hash_key, hash_value       │
                        │ Type 2    │ active_flag                │
                        │           │ start_date, end_date       │
                        └───────────┬───────────────┬────────────┘
                    ∞ (Many)        │               │ ∞ (Many)
                                    ▼               ▼
       [ Bridge Table ]                                   [ Bridge Table ]
         bridge_title_country                              bridge_title_category
         ┌──────────────────────┐                         ┌──────────────────┐
         │ FK  │ show_id        │                         │ FK  │ show_id    │
         │ FK  │ country_id     │                         │ FK  │ category_id│
         └──────────┬───────────┘                         └────────┬─────────┘
                    │                                              │
                    │ (1)                                          │ (1)
                    ▼                                              ▼
       [ Sub-Dimension: Countries ]                      [ Sub-Dimension: Categories ]
          dim_countries_silver                             dim_categories_silver
         ┌──────────────────────┐                         ┌──────────────────┐
         │ PK  │ country_sk     │                         │ PK  │ category_sk│
         │     │ country_id     │                         │     │ category_id│
         │     │ country_name   │                         │     │ category_nm│
         └────────────────────────┘                        └──────────────────┘
```

---

## 🚀 Getting Started

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Access to S3 or cloud storage (for Auto Loader)
- Python 3.10+ with PySpark
- Delta Lake 2.0+

### Setup Steps

#### 1. Create Configuration Table

```python
# Create schema and config table
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.netflix")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.netflix.config_table (
    pipeline_name STRING,
    file_path STRING,
    header BOOLEAN,
    delimiter STRING,
    table_name STRING,
    schema_detail MAP<STRING, STRING>,
    keys ARRAY<STRING>,
    write_mode STRING
)
""")

# Insert Netflix pipeline configuration
spark.sql("""
INSERT INTO workspace.netflix.config_table VALUES (
    'netflix',
    's3://your-bucket/netflix/',  -- or file path
    true,
    ',',
    'netflix',
    map(
        'show_id', 'string',
        'type', 'string',
        'title', 'string',
        'director', 'string',
        'cast', 'string',
        'country', 'string',
        'date_added', 'date',
        'release_year', 'int',
        'rating', 'string',
        'duration', 'string',
        'listed_in', 'string',
        'description', 'string'
    ),
    array('show_id'),
    'overwrite'
)
""")
```

#### 2. Run Bronze Layer

```python
# Option A: Batch Mode (one-time load)
bronze = BronzeLayer.from_config_table("netflix")
raw_df = bronze.read_from_file()
bronze.load_to_bronze_table(raw_df)

# Option B: Streaming Mode (Auto Loader)
bronze = BronzeLayer.from_config_table("netflix")
bronze.s3_auto_loader(checkpoint_location="/Volumes/workspace/netflix/checkpoint_dir/netflix_bronze/")
```

#### 3. Run Silver Layer

```python
silver = SilverLayer.from_config_table("netflix")
silver.process_cdf_stream_to_silver(
    checkpoint_location="/Volumes/workspace/netflix/checkpoint_dir/netflix_silver/"
)
```

#### 4. Run Gold Layer

```python
gold = GoldLayer.from_config_table("netflix")
gold.run_gold_pipeline()
```

---

## 🔧 Configuration

### Auto Loader Settings

```python
# Bronze Layer Auto Loader configuration
checkpoint_location = "/Volumes/workspace/netflix/checkpoint_dir/netflix_bronze/"
schema_location = "/Volumes/workspace/netflix/checkpoint_dir/netflix_bronze_schema/"

# Key options:
- cloudFiles.format: "csv"                          # File format
- cloudFiles.schemaEvolutionMode: "rescue"         # Handle schema changes
- pathGlobFilter: "*.csv"                          # File pattern
- mergeSchema: "true"                              # Allow schema updates
- trigger(availableNow=True)                       # Batch-style streaming
```

### Pipeline Triggers

**Bronze Layer**:
- **Batch**: Manual trigger via `load_to_bronze_table()`
- **Streaming**: `trigger(availableNow=True)` - processes all available data then stops

**Silver Layer**:
- **Incremental**: CDF-based streaming with checkpoint
- **Trigger**: `trigger(availableNow=True)` or continuous streaming

**Gold Layer**:
- **Full Refresh**: Manual trigger via `run_gold_pipeline()`
- **Mode**: `overwrite` - replaces entire table

---

## 🧪 Testing

The project includes a comprehensive test suite in `silver_unit_test.py` that validates:

* ✅ **Data Quality Pipeline** - 8-stage validation checks
* ✅ **Star Schema Structure** - 9-table creation (1 main + 4 sub-dimensions + 4 bridges)
* ✅ **SCD Type 2** - Historical change tracking
* ✅ **Bad Record Handling** - Quarantine and audit trail
* ✅ **Incremental Processing** - CDF-based streaming

For detailed test execution instructions, refer to the test file documentation.

---

## 💡 Usage Examples

### Example 1: Initial Pipeline Setup

```python
from dataclasses import dataclass
from pyspark.sql.functions import *

# 1. Run Bronze Layer (Auto Loader)
bronze = BronzeLayer.from_config_table("netflix")
bronze.s3_auto_loader()

# 2. Run Silver Layer (Quality + Star Schema)
silver = SilverLayer.from_config_table("netflix")
silver.process_cdf_stream_to_silver()

# 3. Run Gold Layer (Business Aggregations)
gold = GoldLayer.from_config_table("netflix")
gold.run_gold_pipeline()

# 4. Verify results
spark.table("workspace.netflix.dim_titles_silver").display()
spark.table("workspace.netflix.netflix_content_by_cast_gold").display()
```

### Example 2: Query Star Schema

```python
# Get all titles with their cast members
spark.sql("""
    SELECT 
        t.title,
        t.type,
        t.release_year,
        c.cast_name
    FROM workspace.netflix.dim_titles_silver t
    INNER JOIN workspace.netflix.bridge_title_cast_silver b 
        ON t.show_id = b.show_id
    INNER JOIN workspace.netflix.dim_cast_silver c 
        ON b.cast_id = c.cast_id
    WHERE t.active_flag = TRUE
    ORDER BY t.release_year DESC
    LIMIT 100
""").display()
```

### Example 3: Monitor Data Quality

```python
# Check bad record statistics
spark.sql("""
    SELECT 
        batch_id,
        load_dt,
        COUNT(*) as bad_record_count,
        explode(reason) as failure_reason
    FROM workspace.netflix.netflix_bronze_bad_record
    GROUP BY batch_id, load_dt, reason
    ORDER BY batch_id DESC
""").display()
```

### Example 4: Analyze Business Trends

```python
# Query Gold layer for yearly content trends
spark.sql("""
    SELECT 
        release_year,
        type,
        total_title,
        LAG(total_title, 1) OVER (PARTITION BY type ORDER BY release_year) as prev_year_count,
        (total_title - LAG(total_title, 1) OVER (PARTITION BY type ORDER BY release_year)) as yoy_change
    FROM workspace.netflix.netflix_yearly_content_trends_gold
    WHERE release_year >= 2015
    ORDER BY release_year DESC, type
""").display()
```

---

## 📈 Performance Metrics

### Pipeline Performance

| Metric | Value | Notes |
|--------|-------|-------|
| **Bronze Ingestion** | 317+ records/sec | Auto Loader with `availableNow` trigger |
| **Silver Transformation** | 8-stage pipeline | Completes in 45-60 seconds for 17K rows |
| **Gold Aggregation** | Sub-5 seconds | Full refresh overwrite mode |
| **Total Tables Created** | 14 tables | 1 Bronze + 10 Silver + 2 Gold + 1 Audit |
| **Star Schema Size** | 9 tables | 1 main + 4 sub-dimensions + 4 bridges |

### Data Quality Metrics

| Stage | Records Processed | Bad Records | Pass Rate |
|-------|-------------------|-------------|-----------|
| Trim & Cast | 17,039 | 31 | 99.82% |
| Invalid Detection | 17,008 | 0 | 100% |
| Null Key Detection | 17,008 | 0 | 100% |
| Duplicate Detection | 17,008 | 1,030 | 93.95% |
| **Final Clean Records** | **15,978** | **1,061** | **93.77%** |

### SCD Type 2 Tracking

- **Initial Load**: 15,978 active records
- **Change Detection**: Hash-based SHA-256 comparison
- **Historical Records**: Preserved with `end_date` and `active_flag = False`
- **Query Performance**: Optimized with `active_flag = True` filter

---

## 🎓 Best Practices

### 1. Configuration Management

✅ **Centralize settings** in `config_table`  
✅ **Use factory methods** (`from_config_table()`) for consistency  
✅ **Version control** configuration changes  
✅ **Document schema mappings** in data dictionaries  

### 2. Data Quality

✅ **Quarantine bad records** - never silently discard  
✅ **Track rejection reasons** with detailed audit trail  
✅ **Monitor bad record trends** over time  
✅ **Alert on quality threshold violations**  

### 3. Incremental Processing

✅ **Enable CDF** on all Bronze tables  
✅ **Use checkpoints** for streaming fault tolerance  
✅ **Prefer `trigger(availableNow=True)`** for cost-efficient batch streaming  
✅ **Monitor checkpoint lag** to detect pipeline delays  

### 4. SCD Type 2 Management

✅ **Always query with `active_flag = TRUE`** for current state  
✅ **Use hash comparison** for efficient change detection  
✅ **Preserve historical records** for audit and time-travel queries  
✅ **Index on surrogate keys** for join performance  

### 5. Gold Layer Optimization

✅ **Pre-aggregate** common business metrics  
✅ **Denormalize** for dashboard query performance  
✅ **Filter for active records only** in aggregations  
✅ **Use full refresh** (`overwrite`) for simplicity  
✅ **Partition** large Gold tables by date or key dimensions  

### 6. Production Deployment

✅ **Schedule pipelines** in sequence (Bronze → Silver → Gold)  
✅ **Implement alerting** for pipeline failures  
✅ **Monitor performance metrics** (throughput, latency)  
✅ **Set up data lineage** tracking  
✅ **Document table dependencies** and refresh schedules  

---

## 🐛 Troubleshooting

### Common Issues

#### Issue 1: Auto Loader Not Detecting New Files

**Symptoms**: New files in S3 folder not processed

**Causes**:
- Checkpoint location already processed those files
- `pathGlobFilter` doesn't match file pattern
- Schema evolution mode incompatible with new columns

**Solution**:
```python
# Option A: Reset checkpoint (CAUTION: reprocesses all files)
dbutils.fs.rm("/Volumes/workspace/netflix/checkpoint_dir/netflix_bronze/", recurse=True)

# Option B: Verify file pattern
bronze = BronzeLayer.from_config_table("netflix")
bronze.s3_auto_loader(checkpoint_location="<new_checkpoint_path>")
```

#### Issue 2: SCD Type 2 Not Creating New Versions

**Symptoms**: Changes not reflected as new records with `active_flag = TRUE`

**Causes**:
- Hash values not changing (columns excluded from hash)
- `load_main_dimension()` not running after data changes
- Business key mismatch in join logic

**Solution**:
```python
# Verify hash generation includes all data columns
silver = SilverLayer.from_config_table("netflix")

# Check columns included in hash_value
# Should exclude: keys, exploded columns (cast, director, country, listed_in), _sk
hash_columns = [col for col in silver.data_col 
                if col not in silver.keys and col not in ["cast", "director", "country", "listed_in"]]
print("Columns in hash_value:", hash_columns)

# Re-run Silver layer
silver.process_cdf_stream_to_silver()
```

#### Issue 3: Bad Records Not Captured

**Symptoms**: Invalid data appearing in Silver layer

**Causes**:
- Quality check stages skipped
- Validation rules not matching data patterns
- Bad record table not initialized

**Solution**:
```python
# Verify bad record table exists
spark.sql("SELECT * FROM workspace.netflix.netflix_bronze_bad_record LIMIT 10").display()

# Check validation rules
silver = SilverLayer.from_config_table("netflix")
print("Invalid rules:", silver.invalid_rule)

# Manually run quality checks on a sample
from pyspark.sql.functions import col
bronze_df = spark.table("workspace.netflix.netflix_bronze")
invalid_df = silver.get_invalid_record(bronze_df)
invalid_df.display()
```

#### Issue 4: Spark Connect Serverless Error (SPARK-55448)

**Symptoms**: `STATE_CONSISTENCY` or `XXSC0` error during Auto Loader

**Cause**: Known Spark Connect synchronization bug

**Solution**:
```python
# Already handled in BronzeLayer.s3_auto_loader()
# Error is caught and treated as success
# Verify data loaded successfully:
spark.table("workspace.netflix.netflix_bronze").count()
```

#### Issue 5: Gold Layer Missing Records

**Symptoms**: Gold tables have fewer records than expected

**Causes**:
- Filtering for `active_flag = TRUE` excludes historical records
- Bridge table joins missing some relationships
- Data not yet propagated from Silver layer

**Solution**:
```python
# Check active vs inactive records in Silver
spark.sql("""
    SELECT 
        active_flag,
        COUNT(*) as record_count
    FROM workspace.netflix.dim_titles_silver
    GROUP BY active_flag
""").display()

# Verify Gold layer recreated after Silver updates
gold = GoldLayer.from_config_table("netflix")
gold.run_gold_pipeline()
```

#### Issue 6: Test Failures

**Symptoms**: SCD Type 2 tests fail with "test data still exists"

**Cause**: Previous test runs left test records in Silver tables

**Solution**:
```python
# Clean old test records
spark.sql("""
DELETE FROM workspace.netflix.dim_titles_silver 
WHERE show_id LIKE 'TEST_SCD_%'
""")

# Re-run tests
tests = SilverLayerTests(...)
tests.test_scd_type2_change_detection()
```

---

## 🤝 Contributing

### Development Workflow

1. **Fork the notebook** or create a new version
2. **Make changes** in your development environment
3. **Run full test suite** to verify changes
4. **Update documentation** if adding new features
5. **Submit for review** (if collaborative project)

### Code Standards

✅ Use PEP 8 style guidelines  
✅ Add docstrings to all methods  
✅ Include unit tests for new features  
✅ Update README for significant changes  
✅ Use type hints where applicable  
✅ Follow dataclass patterns for configuration  

### Testing Requirements

- All 5 tests must pass
- No schema breakage in Silver layer
- Performance benchmarks maintained
- Backward compatibility preserved

---

## 📚 Additional Resources

### Documentation

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Change Data Feed Guide](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [SCD Type 2 Best Practices](https://www.databricks.com/blog/2022/08/22/dimensional-modeling-delta-lake.html)

### Related Files

- `framework` - Main pipeline implementation notebook (BronzeLayer, SilverLayer, GoldLayer)
- `silver_unit_test.py` - Comprehensive unit test suite

### Support

For questions or issues:
1. Review the troubleshooting section above
2. Check test results for error details
3. Examine bad record table for data quality issues
4. Consult Databricks documentation

---

## 📝 License

This project is part of the **Databricks for Data Engineers Bootcamp** training program.

---

## 🎉 Acknowledgments

**Built with**:
- Databricks Unified Analytics Platform
- Apache Spark 3.x
- Delta Lake with Change Data Feed
- Python 3.10+ with dataclasses
- Databricks Auto Loader

**Architecture**:
- Medallion Architecture (Bronze/Silver/Gold)
- Star Schema Design (1+4+4 pattern)
- SCD Type 2 Implementation
- Hash-based CDC

**Key Features**:
- Configurable dataclass-based framework
- 8-stage data quality pipeline
- Incremental processing with CDF
- Production-scale testing
- Performance benchmarking

---

**Last Updated**: January 2026  
**Version**: 2.0  
**Status**: Production Ready ✅

---

*Happy Data Engineering! 🚀*
