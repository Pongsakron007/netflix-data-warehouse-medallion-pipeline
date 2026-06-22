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
- [Data Pipeline Components](#data-pipeline-components)
- [Table Schema](#table-schema)
- [Getting Started](#getting-started)
- [Testing Framework](#testing-framework)
- [Usage Examples](#usage-examples)
- [Performance Metrics](#performance-metrics)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

---

## 🎯 Overview

This project implements a **scalable, production-ready data pipeline** for processing Netflix content data using Databricks and the **Medallion Architecture**. The pipeline ingests raw CSV data, applies comprehensive data quality validation, and transforms it into a **star schema** optimized for analytics and business intelligence.

### Key Features

✅ **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (aggregated) layers  
✅ **Data Quality Validation**: 8-stage quality check pipeline with bad record tracking  
✅ **SCD Type 2**: Historical change tracking with temporal validity  
✅ **Star Schema**: 1 main dimension + 4 sub-dimensions + 4 bridge tables  
✅ **Hash-based Change Detection**: Efficient delta identification  
✅ **Incremental Processing**: Change Data Feed (CDF) for performance  
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

  📁 Source Files                🥉 Bronze Layer              🥈 Silver Layer                   🥇 Gold Layer
  ─────────────                 ───────────────              ───────────────                  ─────────────
       CSV                            │                            │                              │
       JSON          ──────►    Raw Data Store      ──────►   Star Schema        ──────►    Aggregations
      Parquet                   + Metadata                   + Quality Checks                 + Analytics
                                 + CDF Enabled                + SCD Type 2                    + Metrics
                                                              + Normalization

  ┌────────────┐              ┌────────────┐              ┌────────────────┐              ┌────────────┐
  │ netflix.csv│              │  netflix   │              │   dim_titles   │              │  Dashboards│
  │            │  ─────────►  │   _bronze  │  ─────────►  │ + 4 sub-dims   │  ─────────►  │    KPIs    │
  │ (17K rows) │              │            │              │ + 4 bridges    │              │  Reports   │
  └────────────┘              └────────────┘              │ + bad_records  │              └────────────┘
                                    ▲                      └────────────────┘
                                    │
                              config_table
                           (pipeline settings)
```

### Layer Responsibilities

#### 🥉 **Bronze Layer** - Raw Data Ingestion
- **Purpose**: Landing zone for external data
- **Characteristics**: Immutable, append-only, schema-on-read
- **Components**: `BronzeLayer` class
- **Features**:
  - File metadata tracking (`_load_dt`, `_file_name`, `_file_path`, etc.)
  - Change Data Feed (CDF) enabled
  - Support for CSV, JSON, Parquet formats
  - Configurable via `config_table`

#### 🥈 **Silver Layer** - Data Quality & Normalization
- **Purpose**: Clean, validated, business-ready data
- **Characteristics**: Normalized, deduplicated, validated
- **Components**: `SilverLayer` class
- **Features**:
  - 8-stage data quality pipeline
  - Star schema with 9 tables total
  - SCD Type 2 historical tracking
  - Hash-based change detection
  - Bad record audit trail

#### 🥇 **Gold Layer** - Analytics & Aggregations
- **Purpose**: Business-specific aggregations and metrics
- **Characteristics**: Denormalized, pre-aggregated, optimized for queries
- **Examples**: Monthly content additions, top genres, director statistics

---

## 📂 Project Structure

```
Netflix_project/
│
├── framework.ipynb                      # Main pipeline implementation
│   ├── BronzeLayer class                # Bronze ingestion logic
│   ├── SilverLayer class                # Silver transformation logic
│   ├── Bronze documentation (markdown)   # Step-by-step Bronze guide
│   └── Silver documentation (markdown)   # Step-by-step Silver guide
│
├── silver_layer_tests.py                # Comprehensive test suite
│   ├── SilverLayerTests class           # 5 automated test methods
│   └── StarSchemaQueries class          # SQL analytics helpers
│
├── README.md                            # This file
│
└── Data Tables:
    ├── workspace.netflix.config_table              # Pipeline configuration
    ├── workspace.netflix.netflix_bronze            # Raw data (Bronze)
    ├── workspace.netflix.dim_titles_silver         # Main dimension (Silver)
    ├── workspace.netflix.dim_cast_silver           # Cast sub-dimension
    ├── workspace.netflix.dim_directors_silver      # Directors sub-dimension
    ├── workspace.netflix.dim_countries_silver      # Countries sub-dimension
    ├── workspace.netflix.dim_categories_silver     # Genres sub-dimension
    ├── workspace.netflix.bridge_title_cast_silver  # Title-Cast relationships
    ├── workspace.netflix.bridge_title_director_silver
    ├── workspace.netflix.bridge_title_country_silver
    ├── workspace.netflix.bridge_title_category_silver
    └── workspace.netflix.netflix_bronze_bad_record # Bad record audit
```

---

## ⚙️ Data Pipeline Components

### 1. Configuration Management

**Table**: `workspace.netflix.config_table`

```python
# Centralized pipeline configuration
config_table columns:
- pipeline_name: str          # Unique identifier (e.g., "netflix")
- file_path: str              # Source data location
- header: bool                # CSV header row existence
- delimiter: str              # Field separator
- table_name: str             # Target Bronze table
- schema_detail: map          # Column → data type mapping
- keys: array                 # Primary key columns
- write_mode: str             # append/overwrite
```

### 2. Bronze Layer (`BronzeLayer` class)

**Responsibilities**:
- Read data from files (CSV, JSON, Parquet)
- Add metadata columns for lineage
- Initialize Delta tables with CDF
- Append-only loading pattern

**Key Methods**:
- `from_config_table(pipeline_name)` - Factory method
- `read_from_file()` - Load and enrich with metadata
- `load_to_bronze_table(df)` - Append to Bronze table
- `_init_bronze_table()` - First-time table creation

### 3. Silver Layer (`SilverLayer` class)

**Responsibilities**:
- Process incremental changes via CDF
- Apply 8-stage data quality validation
- Normalize into star schema (9 tables)
- Implement SCD Type 2 for change tracking
- Log bad records for auditing

**Key Methods**:

#### Data Quality Pipeline:
1. `trim_data()` - Remove whitespace
2. `change_data_type()` - Type conversions
3. `get_invalid_record()` - Detect invalid values
4. `get_key_null_record()` - Find null keys
5. `get_dup_record()` - Identify duplicates (row & key)
6. `get_all_bad_record()` - Consolidate bad records
7. `load_bad_record()` - Audit trail
8. `get_final_result()` - Extract clean records

#### Star Schema Creation:
- `get_hash_key_value()` - Generate change detection hashes
- `load_sub_dimensions()` - Load master data (cast, directors, etc.)
- `load_bridge_tables()` - Load many-to-many relationships
- `load_main_dimension()` - SCD Type 2 upserts
- `process_cdf_stream_to_silver()` - Orchestrate full pipeline

---

## 📊 Table Schema

### Main Dimension: `dim_titles_silver`

| Column | Type | Description |
|--------|------|-------------|
| `title_sk` | BIGINT | Surrogate key (primary key) |
| `show_id` | STRING | Business key from source |
| `type` | STRING | Movie or TV Show |
| `title` | STRING | Content title |
| `date_added` | DATE | Date added to Netflix |
| `release_year` | INT | Year of release |
| `rating` | STRING | Content rating (PG, R, etc.) |
| `duration` | STRING | Runtime or seasons |
| `description` | STRING | Synopsis |
| `hash_key` | STRING | SHA-256 of business keys |
| `hash_value` | STRING | SHA-256 of data columns |
| `active_flag` | BOOLEAN | Current version indicator |
| `start_date` | TIMESTAMP | Version valid from |
| `end_date` | TIMESTAMP | Version valid to (NULL = current) |
| `load_dt` | DATE | Load date |
| `load_dttm` | TIMESTAMP | Load timestamp |

### Sub-Dimensions

**dim_cast_silver** (36,399 actors):
- `cast_sk`, `cast_name`

**dim_directors_silver** (4,996 directors):
- `director_sk`, `director_name`

**dim_countries_silver** (145 countries):
- `country_sk`, `country_name`

**dim_categories_silver** (73 genres):
- `category_sk`, `category_name`

### Bridge Tables (Many-to-Many Relationships)

**bridge_title_cast_silver** (128,818 relationships):
- `title_sk`, `cast_sk`

**bridge_title_director_silver** (14,039 relationships):
- `title_sk`, `director_sk`

**bridge_title_country_silver** (20,110 relationships):
- `title_sk`, `country_sk`

**bridge_title_category_silver** (38,848 relationships):
- `title_sk`, `category_sk`

### Audit Table: `netflix_bronze_bad_record`

| Column | Type | Description |
|--------|------|-------------|
| All source columns | VARIOUS | Original record |
| `_reason` | ARRAY<STRING> | List of validation failures |
| `batch_id` | INT | Batch identifier |
| `load_dt` | DATE | Rejection date |
| `load_dttm` | TIMESTAMP | Rejection timestamp |

---

## 🚀 Getting Started

### Prerequisites

- Databricks workspace (AWS/Azure/GCP)
- Unity Catalog enabled
- Databricks Runtime 13.0+ or MLR 13.0+
- Python 3.10+
- Access to workspace catalog and schema

### Step 1: Setup Configuration

```python
# Create configuration table
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
config_data = [(
    "netflix",
    "/Volumes/main/default/netflix_data/*.csv",
    True,
    ",",
    "workspace.netflix.netflix_bronze",
    {"show_id": "string", "type": "string", ...},
    ["show_id"],
    "append"
)]

spark.createDataFrame(config_data, schema).write.mode("overwrite").saveAsTable("workspace.netflix.config_table")
```

### Step 2: Run Bronze Layer

```python
from framework import BronzeLayer

# Initialize from configuration
b = BronzeLayer.from_config_table("netflix")

# Read and load data
bronze_df = b.read_from_file()
b.load_to_bronze_table(bronze_df)

# Verify
spark.table("workspace.netflix.netflix_bronze").display()
```

### Step 3: Run Silver Layer

```python
from framework import SilverLayer

# Initialize from configuration
s = SilverLayer.from_config_table("netflix")

# Process data through quality pipeline
s.process_cdf_stream_to_silver(
    checkpoint_location="/checkpoints/netflix_silver"
)
```

### Step 4: Verify Results

```python
# Check main dimension
spark.sql("""
SELECT 
    COUNT(*) as total_titles,
    COUNT(CASE WHEN active_flag THEN 1 END) as active_titles,
    COUNT(DISTINCT show_id) as unique_shows
FROM workspace.netflix.dim_titles_silver
""").display()

# Check data quality
spark.sql("""
SELECT 
    _reason,
    COUNT(*) as count
FROM workspace.netflix.netflix_bronze_bad_record
GROUP BY _reason
ORDER BY count DESC
""").display()
```

---

## 🧪 Testing Framework

### Test Suite Overview

The project includes a comprehensive test suite in `silver_layer_tests.py` with **5 automated tests** covering all pipeline aspects.

### Running Tests

```python
from silver_layer_tests import SilverLayerTests

# Initialize test suite
tests = SilverLayerTests(
    bronze_table="workspace.netflix.netflix_bronze",
    silver_table="workspace.netflix.dim_titles_silver",
    bad_record_table="workspace.netflix.netflix_bronze_bad_record"
)

# Run all tests
results = tests.run_all_tests(skip_full_dataset=False)

# Run individual tests
tests.test_star_schema_integration()
tests.test_complete_pipeline_real_data(batch_size=100)
tests.test_scd_type2_change_detection()
tests.test_full_dataset_performance()
tests.test_idempotency()
```

### Test Descriptions

#### 1. **Star Schema Integration Test**
- **Purpose**: Verify all 9 tables exist and are populated
- **Checks**:
  - Hash key generation
  - Main dimension table
  - 4 sub-dimension tables
  - 4 bridge tables
- **Pass Criteria**: All tables exist with expected record counts

#### 2. **Complete Pipeline Test** (100 records)
- **Purpose**: End-to-end pipeline validation
- **Checks**:
  - Data loading from Bronze
  - Quality check pipeline
  - Good/bad record separation
  - Silver table loading
- **Pass Criteria**: 100% of valid records loaded, bad records logged

#### 3. **SCD Type 2 Change Detection Test**
- **Purpose**: Verify historical change tracking
- **Checks**:
  - Initial record insertion
  - Change detection via hash_value
  - Historical record closure (end_date)
  - New version creation
  - active_flag management
- **Pass Criteria**: Old version closed, new version active, unchanged records unaffected

#### 4. **Full Dataset Performance Test** (17,618 records)
- **Purpose**: Production-scale validation
- **Checks**:
  - Full dataset processing
  - Performance metrics
  - Throughput calculation
- **Pass Criteria**: All records processed within acceptable time (<2 minutes)
- **Results**: **317.7 records/second**, 55.46s total

#### 5. **Idempotency Test**
- **Purpose**: Verify safe re-run capability
- **Checks**:
  - Record counts before/after re-run
  - No duplicate creation
  - Consistent active_flag counts
- **Pass Criteria**: Re-running same data produces NO new records

### Test Results Summary

```
================================================================================
TEST SUITE SUMMARY
================================================================================
   Star Schema Integration       : ✅ PASS
   Complete Pipeline             : ✅ PASS
   SCD Type 2                    : ✅ PASS
   Full Dataset                  : ✅ PASS
   Idempotency                   : ✅ PASS

📊 Results: 5/5 passed, 0 failed, 0 skipped
⏱️  Total time: 184.88s (3.1 min)

🎉 ALL TESTS PASSED - PIPELINE IS PRODUCTION READY!
```

---

## 💡 Usage Examples

### Example 1: Query Active Titles

```sql
SELECT 
    title,
    type,
    release_year,
    rating
FROM workspace.netflix.dim_titles_silver
WHERE active_flag = true
ORDER BY date_added DESC
LIMIT 10
```

### Example 2: Analyze Content by Country

```sql
SELECT 
    c.country_name,
    COUNT(DISTINCT t.title_sk) as title_count,
    SUM(CASE WHEN t.type = 'Movie' THEN 1 ELSE 0 END) as movies,
    SUM(CASE WHEN t.type = 'TV Show' THEN 1 ELSE 0 END) as tv_shows
FROM workspace.netflix.dim_titles_silver t
JOIN workspace.netflix.bridge_title_country_silver b ON t.title_sk = b.title_sk
JOIN workspace.netflix.dim_countries_silver c ON b.country_sk = c.country_sk
WHERE t.active_flag = true
GROUP BY c.country_name
ORDER BY title_count DESC
LIMIT 15
```

### Example 3: Top Actors by Content Count

```sql
SELECT 
    a.cast_name,
    COUNT(DISTINCT t.title_sk) as appearances,
    COUNT(DISTINCT CASE WHEN t.type = 'Movie' THEN t.title_sk END) as movies,
    COUNT(DISTINCT CASE WHEN t.type = 'TV Show' THEN t.title_sk END) as tv_shows
FROM workspace.netflix.dim_cast_silver a
JOIN workspace.netflix.bridge_title_cast_silver b ON a.cast_sk = b.cast_sk
JOIN workspace.netflix.dim_titles_silver t ON b.title_sk = t.title_sk
WHERE t.active_flag = true
GROUP BY a.cast_name
ORDER BY appearances DESC
LIMIT 10
```

### Example 4: Track Historical Changes (SCD Type 2)

```sql
SELECT 
    show_id,
    title,
    rating,
    active_flag,
    start_date,
    end_date,
    CASE 
        WHEN active_flag THEN 'CURRENT'
        ELSE 'HISTORICAL'
    END as version_status
FROM workspace.netflix.dim_titles_silver
WHERE show_id = 's1'  -- Replace with actual show_id
ORDER BY start_date DESC
```

### Example 5: Data Quality Dashboard

```sql
SELECT 
    DATE(load_dttm) as load_date,
    explode(_reason) as failure_reason,
    COUNT(*) as failure_count
FROM workspace.netflix.netflix_bronze_bad_record
GROUP BY DATE(load_dttm), explode(_reason)
ORDER BY load_date DESC, failure_count DESC
```

### Example 6: Using SQL Query Helpers

```python
from silver_layer_tests import StarSchemaQueries

# Quick analytics queries
StarSchemaQueries.query_overview(spark)
StarSchemaQueries.query_top_actors(spark, limit=10)
StarSchemaQueries.query_content_by_country(spark, limit=15)
StarSchemaQueries.query_genre_analysis(spark, limit=15)
StarSchemaQueries.query_multidimensional_analysis(spark, limit=10)
StarSchemaQueries.query_scd_history(spark)
```

---

## 📈 Performance Metrics

### Production Benchmarks

**Test Environment**:
- Platform: Databricks Serverless (AWS)
- Dataset: 17,618 records
- Pipeline: Bronze → Silver (full transformation)

**Results**:

| Metric | Value |
|--------|-------|
| **Total Processing Time** | 55.46 seconds |
| **Throughput** | 317.7 records/second |
| **Average per Record** | 3.15 milliseconds |
| **Data Quality Pass Rate** | 50.0% (8,809 valid) |
| **Bad Records Detected** | 50.0% (8,809 invalid) |
| **Tables Generated** | 9 tables |
| **Relationships Created** | 201,815 total |

**Scalability**:
- ✅ Handles 17K+ records in <1 minute
- ✅ Idempotent (safe to re-run)
- ✅ Incremental processing via CDF
- ✅ Suitable for hourly/daily batch jobs

### Data Distribution

**Star Schema Population**:

| Table | Record Count | Description |
|-------|--------------|-------------|
| dim_titles_silver | 8,817 | Main dimension (8,816 active) |
| dim_cast_silver | 36,399 | Unique actors |
| dim_directors_silver | 4,996 | Unique directors |
| dim_countries_silver | 145 | Countries |
| dim_categories_silver | 73 | Genres |
| bridge_title_cast_silver | 128,818 | Title-actor relationships |
| bridge_title_director_silver | 14,039 | Title-director relationships |
| bridge_title_country_silver | 20,110 | Title-country relationships |
| bridge_title_category_silver | 38,848 | Title-genre relationships |

**Total Relationships**: 201,815 records across bridge tables

---

## ✅ Best Practices

### 1. Data Quality

✅ **Always review bad records**:
```python
spark.sql("""
SELECT _reason, COUNT(*) 
FROM workspace.netflix.netflix_bronze_bad_record 
GROUP BY _reason
""").display()
```

✅ **Monitor quality trends**:
```python
spark.sql("""
SELECT 
    load_dt,
    COUNT(*) as total_processed,
    SUM(CASE WHEN _reason IS NULL THEN 1 ELSE 0 END) as good_records,
    SUM(CASE WHEN _reason IS NOT NULL THEN 1 ELSE 0 END) as bad_records
FROM (
    SELECT load_dt, NULL as _reason FROM workspace.netflix.dim_titles_silver
    UNION ALL
    SELECT load_dt, _reason FROM workspace.netflix.netflix_bronze_bad_record
)
GROUP BY load_dt
ORDER BY load_dt DESC
""").display()
```

### 2. Performance Optimization

✅ **Use incremental processing**:
- Enable CDF on Bronze tables
- Use checkpoints for streaming
- Process only changed records

✅ **Partition large tables**:
```sql
ALTER TABLE workspace.netflix.netflix_bronze 
PARTITION BY (DATE(_load_dt))
```

✅ **Optimize joins**:
- Use broadcast joins for small dimension tables
- Pre-filter before joins
- Use cached tables for repeated queries

### 3. Data Governance

✅ **Document schema changes**:
- Update config_table when source schema changes
- Version control pipeline code
- Maintain audit trail in Bronze

✅ **Set retention policies**:
```sql
ALTER TABLE workspace.netflix.netflix_bronze
SET TBLPROPERTIES (
    'delta.logRetentionDuration' = '90 days',
    'delta.deletedFileRetentionDuration' = '90 days'
)
```

✅ **Implement access controls**:
```sql
GRANT SELECT ON TABLE workspace.netflix.dim_titles_silver TO `analysts`
GRANT SELECT ON TABLE workspace.netflix.netflix_bronze TO `data_engineers`
```

### 4. Monitoring & Alerting

✅ **Track pipeline health**:
- Monitor batch processing times
- Alert on quality check failures
- Track bad record counts
- Monitor table growth rates

✅ **Set up dashboards**:
- Data freshness (last load timestamp)
- Quality metrics (good/bad ratio)
- Schema evolution changes
- Performance trends

### 5. Testing

✅ **Run tests before production deployment**:
```python
# Always run full test suite
results = tests.run_all_tests(skip_full_dataset=False)
assert results['failed'] == 0, "Tests must pass before deployment"
```

✅ **Test with production-like data volumes**
✅ **Validate SCD Type 2 behavior**
✅ **Verify idempotency**

---

## 🔧 Troubleshooting

### Common Issues

#### Issue 1: Schema Mismatch Errors

**Symptom**: `AnalysisException: cannot resolve column`

**Causes**:
- Source CSV columns changed
- Config table schema_detail outdated
- Missing columns in source data

**Solution**:
```python
# 1. Check source schema
df = spark.read.option("header", True).csv("/path/to/file.csv")
df.printSchema()

# 2. Update config_table
spark.sql("""
UPDATE workspace.netflix.config_table
SET schema_detail = map(
    'show_id', 'string',
    'new_column', 'string',
    ...
)
WHERE pipeline_name = 'netflix'
""")

# 3. Recreate Bronze table if needed
spark.sql("DROP TABLE IF EXISTS workspace.netflix.netflix_bronze")
```

#### Issue 2: Duplicate Records

**Symptom**: Bad records with `_key_duplicate` reason

**Causes**:
- Same show_id with different data
- Source data quality issues

**Solution**:
```python
# Investigate duplicates
spark.sql("""
SELECT show_id, COUNT(*) as dup_count
FROM workspace.netflix.netflix_bronze
GROUP BY show_id
HAVING dup_count > 1
""").display()

# Review bad records
spark.sql("""
SELECT *
FROM workspace.netflix.netflix_bronze_bad_record
WHERE array_contains(_reason, '_key_duplicate')
""").display()
```

#### Issue 3: SCD Type 2 Not Closing Old Records

**Symptom**: Multiple active records for same show_id

**Causes**:
- Hash calculation inconsistency
- Missing hash_key or hash_value columns

**Solution**:
```python
# Check for multiple active versions
spark.sql("""
SELECT show_id, COUNT(*) as active_count
FROM workspace.netflix.dim_titles_silver
WHERE active_flag = true
GROUP BY show_id
HAVING active_count > 1
""").display()

# Verify hash generation
from framework import SilverLayer
s = SilverLayer.from_config_table("netflix")
test_df = spark.table("workspace.netflix.netflix_bronze").limit(5)
hash_df = s.get_hash_key_value(test_df)
hash_df.select("show_id", "hash_key", "hash_value").display()
```

#### Issue 4: Performance Degradation

**Symptom**: Processing takes longer than expected

**Causes**:
- Table not optimized
- Full table scans instead of incremental
- Missing CDF checkpoint

**Solution**:
```sql
-- Optimize Delta tables
OPTIMIZE workspace.netflix.netflix_bronze;
OPTIMIZE workspace.netflix.dim_titles_silver;

-- Check table statistics
DESCRIBE DETAIL workspace.netflix.netflix_bronze;

-- Analyze query plans
EXPLAIN EXTENDED
SELECT * FROM workspace.netflix.dim_titles_silver WHERE active_flag = true;
```

#### Issue 5: Test Failures

**Symptom**: SCD Type 2 test failing

**Causes**:
- Old test records persisting
- Schema mismatches in test data

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

### Testing Requirements

- All 5 tests must pass
- No schema breakage in Silver layer
- Performance benchmarks maintained
- Backward compatibility preserved

---

## 📚 Additional Resources

### Documentation

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake Documentation](https://docs.delta.io/)
- [Change Data Feed Guide](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [SCD Type 2 Best Practices](https://www.databricks.com/blog/2022/08/22/dimensional-modeling-delta-lake.html)

### Related Notebooks

- `framework.ipynb` - Main pipeline implementation
- `silver_layer_tests.py` - Comprehensive test suite

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
- Delta Lake
- Python 3.10+

**Architecture**:
- Medallion Architecture (Bronze/Silver/Gold)
- Star Schema Design
- SCD Type 2 Implementation

**Testing**:
- Comprehensive automated test suite
- Production-scale validation
- Performance benchmarking

---

**Last Updated**: June 2026  
**Version**: 1.0  
**Status**: Production Ready ✅

---

*Happy Data Engineering! 🚀*