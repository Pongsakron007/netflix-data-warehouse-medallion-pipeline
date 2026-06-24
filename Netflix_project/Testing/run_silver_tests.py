# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load Framework Classes
# =============================================================================
# SETUP - Load Required Libraries
# =============================================================================
# IMPORTANT: Before running this notebook, you must first run all cells in the
# framework notebook to load the BronzeLayer and SilverLayer classes into the
# shared REPL context.
#
# Framework notebook location:
# /Users/pongsakronk009@hotmail.com/Databricks-for-Data-Engineers-Bootcamp2/Netflix_project/framework
# =============================================================================

from dataclasses import dataclass
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import *
from delta.tables import *
import sys

# Add project path for imports
sys.path.append('/Workspace/Users/pongsakronk009@hotmail.com/Databricks-for-Data-Engineers-Bootcamp2/Netflix_project')

print("✅ Required libraries loaded!")
print("\n⚠️  NOTE: Make sure you've run the framework notebook first to load:")
print("   • BronzeLayer class")
print("   • SilverLayer class")

# COMMAND ----------

# DBTITLE 1,Import Test Classes
# =============================================================================
# IMPORT TEST CLASSES
# =============================================================================
# Import test suite classes from silver_layer_tests.py
# =============================================================================

import sys
import importlib

# Add project path for imports
sys.path.append('/Workspace/Users/pongsakronk009@hotmail.com/Databricks-for-Data-Engineers-Bootcamp2/Netflix_project')

# Import test classes
from silver_layer_tests import SilverLayerTests, StarSchemaQueries

# Reload module to pick up latest changes
if 'silver_layer_tests' in sys.modules:
    importlib.reload(sys.modules['silver_layer_tests'])

print("✅ Test suite imported successfully!")
print("\n📦 Available test methods:")
print("   • test_star_schema_integration()")
print("   • test_complete_pipeline_real_data(batch_size=100)")
print("   • test_scd_type2_change_detection()")
print("   • test_full_dataset_performance()")
print("   • test_idempotency()")
print("   • investigate_bad_records(batch_id)")
print("   • run_all_tests(skip_full_dataset=False)")
print("\n📊 Available SQL query helpers:")
print("   • StarSchemaQueries.query_overview(spark)")
print("   • StarSchemaQueries.query_top_actors(spark, limit=10)")
print("   • StarSchemaQueries.query_content_by_country(spark, limit=15)")
print("   • StarSchemaQueries.query_genre_analysis(spark, limit=15)")
print("   • StarSchemaQueries.query_multidimensional_analysis(spark, limit=10)")
print("   • StarSchemaQueries.query_scd_history(spark)")

# COMMAND ----------

# DBTITLE 1,Clean Test Data
# =============================================================================
# CLEAN TEST DATA - Remove old test records before running tests
# =============================================================================

print("🧹 Cleaning old test data...\n")

# Check existing test records
test_records_before = spark.sql("""
    SELECT COUNT(*) as record_count
    FROM workspace.netflix.dim_titles_silver
    WHERE show_id LIKE 'TEST_SCD_%'
""").collect()[0].record_count

print(f"Found {test_records_before} existing test records")

if test_records_before > 0:
    # Delete old test records
    spark.sql("""
        DELETE FROM workspace.netflix.dim_titles_silver
        WHERE show_id LIKE 'TEST_SCD_%'
    """)
    print(f"✅ Deleted {test_records_before} old test records\n")
else:
    print("✅ No test records to clean\n")

print("Ready to run tests with clean data!")

# COMMAND ----------

# DBTITLE 1,Run All Tests
# =============================================================================
# RUN ALL TESTS - COMPREHENSIVE VALIDATION
# =============================================================================
# This cell runs the complete test suite for the Silver Layer pipeline
# =============================================================================

import time
from datetime import datetime

print("="*80)
print("INITIALIZING TEST SUITE")
print("="*80)
print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Clean old test data before running tests
print("🧹 Cleaning old test data...")
test_records_count = spark.sql("""
    SELECT COUNT(*) as cnt FROM workspace.netflix.dim_titles_silver
    WHERE show_id LIKE 'TEST_SCD_%'
""").collect()[0].cnt

if test_records_count > 0:
    spark.sql("DELETE FROM workspace.netflix.dim_titles_silver WHERE show_id LIKE 'TEST_SCD_%'")
    print(f"✅ Deleted {test_records_count} old test records\n")
else:
    print("✅ No old test data found\n")

# Initialize test suite
silver = SilverLayer.from_config_table("netflix")
tests = SilverLayerTests(silver, spark)

print("✅ Test suite initialized successfully!\n")

# Run all tests (INCLUDING full dataset test)
print("🚀 Running all tests (INCLUDING full dataset test - ~17K records)...\n")
print("⏱️  Note: This may take 2-3 minutes...\n")

start_time = time.time()
results = tests.run_all_tests(skip_full_dataset=False)
total_time = time.time() - start_time

print("\n" + "="*80)
print("✅ TEST SUITE COMPLETED")
print("="*80)
print(f"Total execution time: {total_time:.2f}s ({total_time/60:.1f} min)")
print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
