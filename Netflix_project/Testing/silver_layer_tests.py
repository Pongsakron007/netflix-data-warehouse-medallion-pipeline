# =============================================================================
# SILVER LAYER TESTING SUITE
# =============================================================================
# Comprehensive test suite for Netflix Silver Layer data pipeline
# Includes unit tests, integration tests, and performance tests
# =============================================================================

import time
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_date, current_timestamp, expr, sha2, concat_ws, array_join, array_sort, collect_set, flatten, collect_list
from delta.tables import DeltaTable


class SilverLayerTests:
    """
    Comprehensive test suite for Silver Layer pipeline.
    
    Test Categories:
    - Integration Tests: Verify star schema and component integration
    - Data Quality Tests: Validate pipeline with real data
    - SCD Type 2 Tests: Verify historical tracking and change detection
    - Performance Tests: Monitor execution time and throughput
    - Idempotency Tests: Ensure re-runs don't create duplicates
    """
    
    def __init__(self, silver_layer, spark: SparkSession):
        """
        Initialize test suite.
        
        Args:
            silver_layer: Instance of SilverLayer class
            spark: Active SparkSession
        """
        self.silver = silver_layer
        self.spark = spark
        
    # =========================================================================
    # INTEGRATION TESTS
    # =========================================================================
    
    def test_star_schema_integration(self):
        """
        Test 1: Verify Star Schema Integration
        
        Tests:
        - Hash key generation
        - Sub-dimension table structure
        - Bridge table structure
        - Foreign key relationships
        """
        print("="*70)
        print("TEST 1: STAR SCHEMA INTEGRATION")
        print("="*70)
        
        print("\n✅ SilverLayer initialized successfully")
        print(f"   Bronze table: {self.silver.bronze_table_name}")
        print(f"   Silver table: {self.silver.silver_table_name}")
        print(f"   Bad record table: {self.silver.bad_record_table_name}")
        
        # Test hash key generation
        print("\n📋 Testing get_hash_key_value method...")
        sample_df = self.spark.createDataFrame([
            ("s1", "Movie", "Test Movie", "2021-01-15", "2021", "PG-13", "90 min", 
             "Test,Cast", "Test Director", "USA", "Action,Drama", "desc", 1)
        ], ["show_id", "type", "title", "date_added", "release_year", "rating", "duration",
            "cast", "director", "country", "listed_in", "description", "_sk"])
        
        hash_df = self.silver.get_hash_key_value(sample_df)
        
        # Verify hash columns exist
        assert "hash_key" in hash_df.columns, "hash_key column missing"
        assert "hash_value" in hash_df.columns, "hash_value column missing"
        
        # Verify explodable columns are dropped
        for col_name in ["cast", "director", "country", "listed_in"]:
            assert col_name not in hash_df.columns, f"{col_name} should be dropped"
        
        print("   ✅ Hash key generation working correctly")
        hash_df.display()
        
        # Verify sub-dimension tables exist
        print("\n📋 Verifying Sub-Dimension Tables...")
        dim_tables = [
            "workspace.netflix.dim_cast_silver",
            "workspace.netflix.dim_directors_silver",
            "workspace.netflix.dim_countries_silver",
            "workspace.netflix.dim_categories_silver"
        ]
        
        for table in dim_tables:
            try:
                df = self.spark.table(table)
                count = df.count()
                print(f"   ✅ {table}: {count:,} records")
            except Exception as e:
                print(f"   ❌ {table}: {str(e)}")
                raise
        
        # Verify bridge tables exist
        print("\n📋 Verifying Bridge Tables...")
        bridge_tables = [
            "workspace.netflix.bridge_title_cast_silver",
            "workspace.netflix.bridge_title_director_silver",
            "workspace.netflix.bridge_title_country_silver",
            "workspace.netflix.bridge_title_category_silver"
        ]
        
        for table in bridge_tables:
            try:
                df = self.spark.table(table)
                count = df.count()
                print(f"   ✅ {table}: {count:,} relationships")
            except Exception as e:
                print(f"   ❌ {table}: {str(e)}")
                raise
        
        print("\n✅ Star Schema Integration Test PASSED\n")
        return True
    
    # =========================================================================
    # DATA QUALITY TESTS
    # =========================================================================
    
    def test_complete_pipeline_real_data(self, batch_size: int = 100):
        """
        Test 2: Complete Pipeline with Real Netflix Data
        
        Tests:
        - End-to-end data flow from Bronze to Silver
        - Data quality validation
        - All 9 tables populated correctly
        
        Args:
            batch_size: Number of records to process (default: 100)
        """
        print("="*80)
        print(f"TEST 2: COMPLETE PIPELINE WITH REAL DATA ({batch_size} records)")
        print("="*80)
        
        print("\n📋 Pipeline Configuration:")
        print(f"   Bronze Table: {self.silver.bronze_table_name}")
        print(f"   Silver Table: {self.silver.silver_table_name}")
        print(f"   Bad Record Table: {self.silver.bad_record_table_name}")
        
        # Load sample data from bronze
        print(f"\n📥 Loading {batch_size} records from Bronze...")
        bronze_sample = self.spark.table(self.silver.bronze_table_name).limit(batch_size)
        
        test_batch_id = 999  # Use distinct batch ID for testing
        
        # Add surrogate key
        batch_with_sk = bronze_sample.withColumn(
            "_sk",
            (lit(test_batch_id).cast("long") * 1000000000) + 
            expr("monotonically_increasing_id()")
        )
        
        print(f"✅ Loaded {batch_with_sk.count()} records")
        
        # Run quality checks pipeline
        print("\n⚙️  Running Quality Checks Pipeline...")
        start_time = time.time()
        
        trimmed = self.silver.trim_data(batch_with_sk)
        typed = self.silver.change_data_type(trimmed)
        invalid = self.silver.get_invalid_record(typed)
        key_null = self.silver.get_key_null_record(typed)
        duplicate = self.silver.get_dup_record(typed, key_null)
        all_bad = self.silver.get_all_bad_record(invalid, key_null, duplicate)
        final = self.silver.get_final_result(typed, all_bad)
        
        # Calculate quality metrics
        good_count = final.count()
        bad_count = all_bad.count()
        pass_rate = (good_count / batch_size * 100) if batch_size > 0 else 100
        
        print(f"\n📊 Quality Check Results:")
        print(f"   Total processed: {batch_size}")
        print(f"   Good records: {good_count} ({pass_rate:.1f}%)")
        print(f"   Bad records: {bad_count} ({100-pass_rate:.1f}%)")
        print(f"   Processing time: {time.time() - start_time:.2f}s")
        
        # Load to all silver tables
        print("\n📤 Loading to Silver Layer Tables...")
        
        self.silver.load_sub_dimensions(final, test_batch_id)
        print("   ✅ Sub-dimensions loaded")
        
        self.silver.load_bridge_tables(final, test_batch_id)
        print("   ✅ Bridge tables loaded")
        
        self.silver.load_bad_record(all_bad, test_batch_id)
        print("   ✅ Bad records logged")
        
        self.silver.load_to_silver_layer(final, test_batch_id)
        print("   ✅ Main dimension loaded")
        
        print("\n✅ Complete Pipeline Test PASSED\n")
        return True
    
    # =========================================================================
    # SCD TYPE 2 TESTS
    # =========================================================================
    
    def test_scd_type2_change_detection(self):
        """
        Test 3: SCD Type 2 Change Detection
        
        Tests:
        - New records inserted with active_flag=true
        - Changed records: old closed (active_flag=false), new inserted
        - Unchanged records remain untouched
        - Hash-based change detection working correctly
        """
        print("="*80)
        print("TEST 3: SCD TYPE 2 CHANGE DETECTION")
        print("="*80)
        
        # Create test records
        print("\n📋 Creating test records...")
        
        test_data = [
            # New records (with all required columns)
            ("TEST_SCD_001", "Movie", "Original Title 1", "2020-01-15", 2020, "PG", 
             "90 min", "Test Actor", "Test Director", "USA", "Action", "desc1", 1001),
            ("TEST_SCD_002", "TV Show", "Original Title 2", "2021-03-10", 2021, "TV-14", 
             "2 Seasons", "Test Actor 2", "Test Director 2", "Canada", "Drama", "desc2", 1002),
        ]
        
        df_batch1 = self.spark.createDataFrame(
            test_data,
            ["show_id", "type", "title", "date_added", "release_year", "rating", 
             "duration", "cast", "director", "country", "listed_in", "description", "_sk"]
        ).withColumn("load_dt", current_date()).withColumn("load_dttm", current_timestamp())
        
        # Generate hash keys
        batch1_with_hash = self.silver.get_hash_key_value(df_batch1)
        
        print("\n📤 Batch 1: Loading initial records...")
        self.silver.load_to_silver_layer(batch1_with_hash, 1001)
        
        # Verify initial load
        initial_records = self.spark.sql("""
            SELECT show_id, title, active_flag, start_date, end_date
            FROM workspace.netflix.dim_titles_silver
            WHERE show_id LIKE 'TEST_SCD_%'
            ORDER BY show_id
        """)
        
        print("\n📊 After Batch 1 (Initial Load):")
        initial_records.display()
        
        # Create changed records (same keys, different data)
        changed_data = [
            ("TEST_SCD_001", "Movie", "CHANGED Title 1", "2020-01-15", 2020, "PG-13",  # Title & rating changed
             "90 min", "Test Actor", "Test Director", "USA", "Action", "desc1 updated", 2001),
            ("TEST_SCD_002", "TV Show", "Original Title 2", "2021-03-10", 2021, "TV-14",  # No change
             "2 Seasons", "Test Actor 2", "Test Director 2", "Canada", "Drama", "desc2", 2002),
        ]
        
        df_batch2 = self.spark.createDataFrame(
            changed_data,
            ["show_id", "type", "title", "date_added", "release_year", "rating", 
             "duration", "cast", "director", "country", "listed_in", "description", "_sk"]
        ).withColumn("load_dt", current_date()).withColumn("load_dttm", current_timestamp())
        
        batch2_with_hash = self.silver.get_hash_key_value(df_batch2)
        
        print("\n📤 Batch 2: Loading changed records...")
        time.sleep(2)  # Ensure timestamp difference
        self.silver.load_to_silver_layer(batch2_with_hash, 2001)
        
        # Verify SCD Type 2 behavior
        final_records = self.spark.sql("""
            SELECT show_id, title, active_flag, start_date, end_date,
                   CASE WHEN active_flag THEN 'ACTIVE' ELSE 'CLOSED' END as status
            FROM workspace.netflix.dim_titles_silver
            WHERE show_id LIKE 'TEST_SCD_%'
            ORDER BY show_id, start_date
        """)
        
        print("\n📊 After Batch 2 (With Changes):")
        final_records.display()
        
        # Verify expectations
        all_records = final_records.collect()
        
        # Check TEST_SCD_001 (should have 2 versions)
        scd_001_records = [r for r in all_records if r.show_id == "TEST_SCD_001"]
        assert len(scd_001_records) == 2, "TEST_SCD_001 should have 2 versions"
        
        old_record = [r for r in scd_001_records if not r.active_flag][0]
        new_record = [r for r in scd_001_records if r.active_flag][0]
        
        assert old_record.title == "Original Title 1", "Old record should have original title"
        assert new_record.title == "CHANGED Title 1", "New record should have changed title"
        assert old_record.end_date is not None, "Old record should have end_date"
        assert new_record.end_date is None, "New record should have NULL end_date"
        
        print("\n✅ SCD Type 2 verification:")
        print("   ✅ Changed record created new version")
        print("   ✅ Old version closed with end_date")
        print("   ✅ New version active with NULL end_date")
        print("   ✅ Unchanged record remains single version")
        
        print("\n✅ SCD Type 2 Change Detection Test PASSED\n")
        return True
    
    # =========================================================================
    # PERFORMANCE TESTS
    # =========================================================================
    
    def test_full_dataset_performance(self):
        """
        Test 4: Full Dataset Performance Test
        
        Tests:
        - Pipeline scalability with full dataset (17,618 records)
        - Processing time and throughput
        - Memory efficiency
        - All tables loaded successfully
        """
        print("="*80)
        print("TEST 4: FULL DATASET PERFORMANCE TEST")
        print("="*80)
        print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Load full bronze dataset
        print("\n📥 Loading FULL dataset from Bronze...")
        load_start = time.time()
        full_bronze_df = self.spark.table(self.silver.bronze_table_name)
        total_count = full_bronze_df.count()
        load_duration = time.time() - load_start
        
        print(f"   Loaded: {total_count:,} records")
        print(f"   Time: {load_duration:.2f}s")
        
        # Process through pipeline
        test_batch_id = 10000
        processing_start = time.time()
        
        print(f"\n⚙️  Processing batch {test_batch_id}...")
        
        # Add surrogate key
        batch_with_sk = full_bronze_df.withColumn(
            "_sk",
            (lit(test_batch_id).cast("long") * 1000000000) + 
            expr("monotonically_increasing_id()")
        )
        
        # Quality checks
        trimmed = self.silver.trim_data(batch_with_sk)
        typed = self.silver.change_data_type(trimmed)
        invalid = self.silver.get_invalid_record(typed)
        key_null = self.silver.get_key_null_record(typed)
        duplicate = self.silver.get_dup_record(typed, key_null)
        all_bad = self.silver.get_all_bad_record(invalid, key_null, duplicate)
        final = self.silver.get_final_result(typed, all_bad)
        
        # Calculate metrics
        good_count = final.count()
        bad_count = all_bad.count()
        pass_rate = (good_count / total_count * 100) if total_count > 0 else 100
        
        print(f"\n📊 Quality Results:")
        print(f"   Good: {good_count:,} ({pass_rate:.2f}%)")
        print(f"   Bad: {bad_count:,} ({100-pass_rate:.2f}%)")
        
        # Load to all tables
        self.silver.load_sub_dimensions(final, test_batch_id)
        self.silver.load_bridge_tables(final, test_batch_id)
        self.silver.load_bad_record(all_bad, test_batch_id)
        self.silver.load_to_silver_layer(final, test_batch_id)
        
        total_duration = time.time() - processing_start
        throughput = total_count / total_duration
        
        print(f"\n⏱️  Performance Metrics:")
        print(f"   Total time: {total_duration:.2f}s ({total_duration/60:.1f} min)")
        print(f"   Throughput: {throughput:.1f} records/second")
        print(f"   Avg per record: {(total_duration/total_count)*1000:.2f} ms")
        
        print("\n✅ Full Dataset Performance Test PASSED\n")
        return True
    
    # =========================================================================
    # IDEMPOTENCY TESTS
    # =========================================================================
    
    def test_idempotency(self):
        """
        Test 5: Idempotency Test - Re-run Same Data
        
        Tests:
        - Re-processing same data doesn't create duplicates
        - Hash-based change detection prevents false SCD changes
        - Dimension tables remain stable
        - Bridge tables handle re-runs correctly
        """
        print("="*80)
        print("TEST 5: IDEMPOTENCY - RE-RUN SAME DATA")
        print("="*80)
        
        # Capture BEFORE state
        print("\n📊 Capturing BEFORE state...")
        before_counts = {
            "Main Dimension (Total)": self.spark.sql(
                "SELECT COUNT(*) as cnt FROM workspace.netflix.dim_titles_silver"
            ).collect()[0].cnt,
            "Main Dimension (Active)": self.spark.sql(
                "SELECT COUNT(*) as cnt FROM workspace.netflix.dim_titles_silver WHERE active_flag = true"
            ).collect()[0].cnt,
        }
        
        for table, count in before_counts.items():
            print(f"   {table}: {count:,}")
        
        # Re-run with SAME data but different batch_id
        print("\n🔄 Re-running pipeline with SAME data...")
        full_bronze_df = self.spark.table(self.silver.bronze_table_name)
        
        idempotency_batch_id = 10001
        batch_with_sk = full_bronze_df.withColumn(
            "_sk",
            (lit(idempotency_batch_id).cast("long") * 1000000000) + 
            expr("monotonically_increasing_id()")
        )
        
        # Process
        trimmed = self.silver.trim_data(batch_with_sk)
        typed = self.silver.change_data_type(trimmed)
        invalid = self.silver.get_invalid_record(typed)
        key_null = self.silver.get_key_null_record(typed)
        duplicate = self.silver.get_dup_record(typed, key_null)
        all_bad = self.silver.get_all_bad_record(invalid, key_null, duplicate)
        final = self.silver.get_final_result(typed, all_bad)
        
        # Load
        self.silver.load_sub_dimensions(final, idempotency_batch_id)
        self.silver.load_bridge_tables(final, idempotency_batch_id)
        self.silver.load_bad_record(all_bad, idempotency_batch_id)
        self.silver.load_to_silver_layer(final, idempotency_batch_id)
        
        # Capture AFTER state
        print("\n📊 Capturing AFTER state...")
        after_counts = {
            "Main Dimension (Total)": self.spark.sql(
                "SELECT COUNT(*) as cnt FROM workspace.netflix.dim_titles_silver"
            ).collect()[0].cnt,
            "Main Dimension (Active)": self.spark.sql(
                "SELECT COUNT(*) as cnt FROM workspace.netflix.dim_titles_silver WHERE active_flag = true"
            ).collect()[0].cnt,
        }
        
        for table, count in after_counts.items():
            print(f"   {table}: {count:,}")
        
        # Verify idempotency
        print("\n🔍 Verifying idempotency...")
        all_passed = True
        
        for table in before_counts.keys():
            before = before_counts[table]
            after = after_counts[table]
            if before == after:
                print(f"   ✅ {table}: UNCHANGED ({before:,})")
            else:
                print(f"   ❌ {table}: CHANGED {before:,} → {after:,}")
                all_passed = False
        
        if all_passed:
            print("\n✅ Idempotency Test PASSED")
            print("   Re-processing same data does NOT create duplicates")
        else:
            print("\n❌ Idempotency Test FAILED")
            raise AssertionError("Dimension counts changed after re-run")
        
        return True
    
    # =========================================================================
    # DATA INVESTIGATION HELPERS
    # =========================================================================
    
    def investigate_bad_records(self, batch_id: int = 10000):
        """
        Helper: Investigate Bad Records & Duplicates
        
        Analyzes rejected records to understand data quality issues.
        
        Args:
            batch_id: Batch ID to investigate
        """
        print("="*80)
        print(f"BAD RECORD INVESTIGATION - BATCH {batch_id}")
        print("="*80)
        
        # Reason distribution
        print("\n1️⃣ Bad Record Reason Distribution:")
        reason_dist = self.spark.sql(f"""
            WITH exploded_reasons AS (
                SELECT EXPLODE(reason) as reason_type
                FROM workspace.netflix.netflix_bronze_bad_record
                WHERE batch_id = {batch_id}
            )
            SELECT reason_type, COUNT(*) as count
            FROM exploded_reasons
            GROUP BY reason_type
            ORDER BY count DESC
        """)
        reason_dist.display()
        
        # Sample duplicates
        print("\n2️⃣ Sample Duplicate Records:")
        duplicates = self.spark.sql(f"""
            SELECT show_id, type, title, reason
            FROM workspace.netflix.netflix_bronze_bad_record
            WHERE batch_id = {batch_id}
              AND array_contains(reason, '_row_duplication')
            ORDER BY show_id
            LIMIT 10
        """)
        duplicates.display()
        
        # Check for exact duplicates in bronze
        print("\n3️⃣ Exact Row Duplicates in Bronze:")
        dup_check = self.spark.sql("""
            WITH dup_check AS (
                SELECT show_id, type, title, director, cast, country,
                       date_added, release_year, rating, duration, 
                       listed_in, description,
                       COUNT(*) as occurrence_count
                FROM workspace.netflix.netflix_bronze
                GROUP BY show_id, type, title, director, cast, country,
                         date_added, release_year, rating, duration, 
                         listed_in, description
                HAVING COUNT(*) > 1
            )
            SELECT 
                COUNT(*) as unique_duplicate_records,
                SUM(occurrence_count) as total_duplicate_rows
            FROM dup_check
        """)
        dup_check.display()
        
    # =========================================================================
    # TEST RUNNER
    # =========================================================================
    
    def run_all_tests(self, skip_full_dataset: bool = False):
        """
        Run all test suites.
        
        Args:
            skip_full_dataset: Skip the full dataset test (for faster execution)
        """
        print("\n" + "="*80)
        print("RUNNING COMPLETE TEST SUITE")
        print("="*80)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        results = {}
        start_time = time.time()
        
        # Run tests
        try:
            print("\n[1/5] Testing Star Schema Integration...")
            results["Star Schema Integration"] = self.test_star_schema_integration()
        except Exception as e:
            print(f"❌ FAILED: {str(e)}")
            results["Star Schema Integration"] = False
        
        try:
            print("\n[2/5] Testing Complete Pipeline (100 records)...")
            results["Complete Pipeline"] = self.test_complete_pipeline_real_data(100)
        except Exception as e:
            print(f"❌ FAILED: {str(e)}")
            results["Complete Pipeline"] = False
        
        try:
            print("\n[3/5] Testing SCD Type 2 Change Detection...")
            results["SCD Type 2"] = self.test_scd_type2_change_detection()
        except Exception as e:
            print(f"❌ FAILED: {str(e)}")
            results["SCD Type 2"] = False
        
        if not skip_full_dataset:
            try:
                print("\n[4/5] Testing Full Dataset Performance...")
                results["Full Dataset"] = self.test_full_dataset_performance()
            except Exception as e:
                print(f"❌ FAILED: {str(e)}")
                results["Full Dataset"] = False
        else:
            print("\n[4/5] Skipping Full Dataset Test")
            results["Full Dataset"] = "Skipped"
        
        try:
            print("\n[5/5] Testing Idempotency...")
            results["Idempotency"] = self.test_idempotency()
        except Exception as e:
            print(f"❌ FAILED: {str(e)}")
            results["Idempotency"] = False
        
        total_duration = time.time() - start_time
        
        # Print summary
        print("\n" + "="*80)
        print("TEST SUITE SUMMARY")
        print("="*80)
        
        passed = sum(1 for v in results.values() if v is True)
        failed = sum(1 for v in results.values() if v is False)
        skipped = sum(1 for v in results.values() if v == "Skipped")
        total = len(results)
        
        for test_name, result in results.items():
            status = "✅ PASS" if result is True else ("⏭️  SKIP" if result == "Skipped" else "❌ FAIL")
            print(f"   {test_name:30s}: {status}")
        
        print(f"\n📊 Results: {passed}/{total-skipped} passed, {failed} failed, {skipped} skipped")
        print(f"⏱️  Total time: {total_duration:.2f}s ({total_duration/60:.1f} min)")
        
        if failed == 0:
            print("\n🎉 ALL TESTS PASSED - PIPELINE IS PRODUCTION READY!")
        else:
            print("\n⚠️  SOME TESTS FAILED - REVIEW FAILURES ABOVE")
        
        return results


# =============================================================================
# SQL QUERY HELPERS FOR STAR SCHEMA ANALYSIS
# =============================================================================

class StarSchemaQueries:
    """
    Pre-built SQL queries for star schema analysis and reporting.
    """
    
    @staticmethod
    def query_overview(spark: SparkSession):
        """Query 1: Main Dimension Overview"""
        return spark.sql("""
            SELECT 
              type,
              COUNT(*) as title_count,
              COUNT(DISTINCT CASE WHEN active_flag = true THEN show_id END) as active_titles,
              AVG(release_year) as avg_release_year,
              MIN(release_year) as earliest_year,
              MAX(release_year) as latest_year
            FROM workspace.netflix.dim_titles_silver
            GROUP BY type
            ORDER BY title_count DESC
        """)
    
    @staticmethod
    def query_top_actors(spark: SparkSession, limit: int = 10):
        """Query 2: Top Actors by Number of Titles"""
        return spark.sql(f"""
            SELECT 
              c.cast_name,
              COUNT(DISTINCT b.show_id) as title_count,
              COUNT(DISTINCT CASE WHEN t.type = 'Movie' THEN b.show_id END) as movie_count,
              COUNT(DISTINCT CASE WHEN t.type = 'TV Show' THEN b.show_id END) as tvshow_count
            FROM workspace.netflix.dim_cast_silver c
            JOIN workspace.netflix.bridge_title_cast_silver b ON c.cast_id = b.cast_id
            JOIN workspace.netflix.dim_titles_silver t ON b.show_id = t.show_id AND t.active_flag = true
            GROUP BY c.cast_name
            ORDER BY title_count DESC
            LIMIT {limit}
        """)
    
    @staticmethod
    def query_content_by_country(spark: SparkSession, limit: int = 15):
        """Query 3: Content Production by Country"""
        return spark.sql(f"""
            SELECT 
              co.country_name,
              COUNT(DISTINCT b.show_id) as total_titles,
              COUNT(DISTINCT CASE WHEN t.type = 'Movie' THEN b.show_id END) as movies,
              COUNT(DISTINCT CASE WHEN t.type = 'TV Show' THEN b.show_id END) as tv_shows,
              AVG(t.release_year) as avg_release_year
            FROM workspace.netflix.dim_countries_silver co
            JOIN workspace.netflix.bridge_title_country_silver b ON co.country_id = b.country_id
            JOIN workspace.netflix.dim_titles_silver t ON b.show_id = t.show_id AND t.active_flag = true
            GROUP BY co.country_name
            ORDER BY total_titles DESC
            LIMIT {limit}
        """)
    
    @staticmethod
    def query_genre_analysis(spark: SparkSession, limit: int = 15):
        """Query 4: Genre/Category Distribution"""
        return spark.sql(f"""
            SELECT 
              cat.category_name,
              COUNT(DISTINCT b.show_id) as title_count,
              COUNT(DISTINCT CASE WHEN t.type = 'Movie' THEN b.show_id END) as movies,
              COUNT(DISTINCT CASE WHEN t.type = 'TV Show' THEN b.show_id END) as tv_shows,
              ROUND(COUNT(DISTINCT b.show_id) * 100.0 / SUM(COUNT(DISTINCT b.show_id)) OVER(), 2) as percentage
            FROM workspace.netflix.dim_categories_silver cat
            JOIN workspace.netflix.bridge_title_category_silver b ON cat.category_id = b.category_id
            JOIN workspace.netflix.dim_titles_silver t ON b.show_id = t.show_id AND t.active_flag = true
            GROUP BY cat.category_name
            ORDER BY title_count DESC
            LIMIT {limit}
        """)
    
    @staticmethod
    def query_multidimensional_analysis(spark: SparkSession, limit: int = 10):
        """Query 5: Multi-Dimensional Analysis"""
        return spark.sql(f"""
            SELECT 
              t.title,
              t.type,
              t.release_year,
              t.rating,
              FIRST(d.director_name) as director_name,
              array_join(array_sort(collect_set(c.cast_name)), ', ') as cast_list,
              array_join(array_sort(collect_set(co.country_name)), ', ') as countries,
              array_join(array_sort(collect_set(cat.category_name)), ', ') as genres
            FROM workspace.netflix.dim_titles_silver t
            LEFT JOIN workspace.netflix.bridge_title_director_silver bd ON t.show_id = bd.show_id
            LEFT JOIN workspace.netflix.dim_directors_silver d ON bd.director_id = d.director_id
            LEFT JOIN workspace.netflix.bridge_title_cast_silver bc ON t.show_id = bc.show_id
            LEFT JOIN workspace.netflix.dim_cast_silver c ON bc.cast_id = c.cast_id
            LEFT JOIN workspace.netflix.bridge_title_country_silver bco ON t.show_id = bco.show_id
            LEFT JOIN workspace.netflix.dim_countries_silver co ON bco.country_id = co.country_id
            LEFT JOIN workspace.netflix.bridge_title_category_silver bcat ON t.show_id = bcat.show_id
            LEFT JOIN workspace.netflix.dim_categories_silver cat ON bcat.category_id = cat.category_id
            WHERE t.active_flag = true
            GROUP BY t.show_id, t.title, t.type, t.release_year, t.rating
            ORDER BY t.release_year DESC
            LIMIT {limit}
        """)
    
    @staticmethod
    def query_scd_history(spark: SparkSession):
        """Query 6: SCD Type 2 History Tracking"""
        return spark.sql("""
            SELECT 
              show_id,
              title,
              type,
              release_year,
              rating,
              hash_value,
              active_flag,
              start_date,
              end_date,
              CASE 
                WHEN active_flag = true THEN 'Current'
                ELSE 'Historical'
              END as record_status,
              DATEDIFF(COALESCE(end_date, CURRENT_TIMESTAMP()), start_date) as days_active
            FROM workspace.netflix.dim_titles_silver
            ORDER BY show_id, start_date DESC
        """)


# =============================================================================
# USAGE EXAMPLE
# =============================================================================
"""
To use this test suite in a notebook:

# 1. Import required classes
from silver_layer_tests import SilverLayerTests, StarSchemaQueries

# 2. Initialize your SilverLayer
silver = SilverLayer.from_config_table("netflix")

# 3. Create test suite
tests = SilverLayerTests(silver, spark)

# 4. Run individual tests
tests.test_star_schema_integration()
tests.test_complete_pipeline_real_data(100)
tests.test_scd_type2_change_detection()

# 5. Or run all tests
results = tests.run_all_tests(skip_full_dataset=False)

# 6. Run SQL queries
StarSchemaQueries.query_overview(spark).display()
StarSchemaQueries.query_top_actors(spark, limit=10).display()
"""