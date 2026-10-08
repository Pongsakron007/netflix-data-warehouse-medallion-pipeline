import unittest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import col, count
import sys
import os
# 1. Add import to silver_unit_test.py
from delta import configure_spark_with_delta_pip

# Import logic that works in GitHub Runner, Local, and Databricks
try:
    from unified_fw.fw import GoldLayer
except ImportError:
    # 1. check whether __file__  (GitHub Runner/Local Yes, Databricks use os.getcwd())
    if "__file__" in globals():
        current_dir = os.path.dirname(os.path.abspath(__file__))
    else:
        current_dir = os.getcwd()

    # 2. find location project_root by try go back 1 step
    project_root = os.path.abspath(os.path.join(current_dir, ".."))
    package_path = os.path.join(project_root, "logic_packages", "src")

    # If go back but don't find (in case Root directly) use current folder
    if not os.path.exists(package_path):
        package_path = os.path.join(current_dir, "logic_packages", "src")

    # 3. ADD sys.path
    if package_path not in sys.path:
        sys.path.insert(0, package_path)

    from unified_fw.fw import GoldLayer

class TestGoldLayerWithMocks(unittest.TestCase):
    """Test GoldLayer with mocked Spark tables to avoid using real data."""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session once for all tests."""
        cls.spark = SparkSession.getActiveSession()
        if cls.spark is None:
            builder = (
                SparkSession.builder
                .appName("GoldLayerUnitTests")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
            # configure_spark_with_delta_pip จะใส่ JAR package ให้ตรงกับ pip package โดยอัตโนมัติ
            cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        # Don't stop the session on Spark Connect/Serverless - it's shared
        pass
    
    def setUp(self):
        """Set up test fixtures before each test."""
        self.spark.sql("DROP TABLE IF EXISTS test_content_by_cast_gold")
        self.spark.sql("DROP TABLE IF EXISTS test_yearly_content_trends_gold")
    
    def tearDown(self):
        """Clean up after each test."""
        self.spark.sql("DROP TABLE IF EXISTS test_content_by_cast_gold")
        self.spark.sql("DROP TABLE IF EXISTS test_yearly_content_trends_gold")
    
    def _create_mock_titles_df(self, data=None):
        """Helper to create mock titles DataFrame."""
        schema = StructType([
            StructField("show_id", StringType(), True),
            StructField("title_version_sk", StringType(), True),
            StructField("title", StringType(), True),
            StructField("type", StringType(), True),
            StructField("release_year", IntegerType(), True),
            StructField("active_flag", BooleanType(), True)
        ])
        
        if data is None:
            data = [
                ("s1", "sk_s1", "Stranger Things", "TV Show", 2016, True),
                ("s2", "sk_s2", "The Crown", "TV Show", 2016, True),
                ("m1", "sk_m1", "Bird Box", "Movie", 2018, True)
            ]
        
        return self.spark.createDataFrame(data, schema)
    
    def _create_mock_cast_df(self, data=None):
        """Helper to create mock cast DataFrame."""
        schema = StructType([
            StructField("cast_id", StringType(), True),
            StructField("cast_name", StringType(), True)
        ])
        
        if data is None:
            data = [
                ("cast_A", "Millie Bobby Brown"),
                ("cast_B", "Winona Ryder"),
                ("cast_C", "Claire Foy")
            ]
        
        return self.spark.createDataFrame(data, schema)
    
    def _create_mock_bridge_df(self, data=None):
        """Helper to create mock bridge DataFrame."""
        schema = StructType([
            StructField("show_id", StringType(), True),
            StructField("title_version_sk", StringType(), True),
            StructField("cast_id", StringType(), True)
        ])
        
        if data is None:
            data = [
                ("s1", "sk_s1", "cast_A"),
                ("s1", "sk_s1", "cast_B"),
                ("s2", "sk_s2", "cast_C")
            ]
        
        return self.spark.createDataFrame(data, schema)

    def test_initialization(self):
        """Test GoldLayer initializes correctly."""
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        self.assertEqual(gold.table_name, "test")
        self.assertEqual(gold.keys, ["show_id"])
        self.assertEqual(gold.write_mode, "overwrite")
        self.assertEqual(gold.gold_table_content_by_cast, "test_content_by_cast_gold")
        self.assertEqual(gold.gold_yearly_content_trends, "test_yearly_content_trends_gold")
        self.assertIsNotNone(gold.spark)

    def test_empty_titles_table(self):
        """Test behavior when titles table is empty."""
        # Create empty mock tables
        empty_titles = self._create_mock_titles_df(data=[])
        empty_cast = self._create_mock_cast_df(data=[])
        empty_bridge = self._create_mock_bridge_df(data=[])
        
        # Register as temporary views (simple names required for Spark Connect)
        empty_titles.createOrReplaceTempView("dim_titles_silver")
        empty_cast.createOrReplaceTempView("dim_cast_silver")
        empty_bridge.createOrReplaceTempView("bridge_title_cast_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        # Should not raise error, just create empty table
        gold.create_gold_content_by_cast()
        
        result = self.spark.table("test_content_by_cast_gold")
        self.assertEqual(result.count(), 0, "Empty input should produce empty output table")

    def test_no_active_records(self):
        """Test when all titles are inactive (active_flag=False)."""
        # Create data with all inactive records
        inactive_data = [
            ("s1", "sk_s1_old", "Old Show", "TV Show", 2010, False),
            ("s2", "sk_s2_old", "Old Movie", "Movie", 2011, False)
        ]
        
        titles_df = self._create_mock_titles_df(data=inactive_data)
        cast_df = self._create_mock_cast_df()
        bridge_df = self._create_mock_bridge_df()
        
        titles_df.createOrReplaceTempView("dim_titles_silver")
        cast_df.createOrReplaceTempView("dim_cast_silver")
        bridge_df.createOrReplaceTempView("bridge_title_cast_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        gold.create_gold_content_by_cast()
        
        result = self.spark.table("test_content_by_cast_gold")
        self.assertEqual(result.count(), 0, "Inactive records should be filtered out")

    def test_missing_cast_in_bridge(self):
        """Test when bridge table references non-existent cast."""
        titles_df = self._create_mock_titles_df()
        cast_df = self._create_mock_cast_df()
        
        # Bridge references cast_id=cast_999 which doesn't exist in cast table
        invalid_bridge_data = [
            ("s1", "sk_s1", "cast_A"),
            ("s1", "sk_s1", "cast_999")  # Invalid cast_id
        ]
        bridge_df = self._create_mock_bridge_df(data=invalid_bridge_data)
        
        titles_df.createOrReplaceTempView("dim_titles_silver")
        cast_df.createOrReplaceTempView("dim_cast_silver")
        bridge_df.createOrReplaceTempView("bridge_title_cast_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        gold.create_gold_content_by_cast()
        
        result = self.spark.table("test_content_by_cast_gold")
        # Inner join should drop the invalid cast_id=999
        self.assertEqual(result.count(), 1, "Invalid cast references should be dropped by inner join")

    def test_yearly_trends_with_empty_data(self):
        """Test yearly trends aggregation with empty input."""
        empty_titles = self._create_mock_titles_df(data=[])
        empty_titles.createOrReplaceTempView("dim_titles_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        gold.create_gold_yearly_content_trends()
        
        result = self.spark.table("test_yearly_content_trends_gold")
        self.assertEqual(result.count(), 0, "Empty input should produce empty trends table")

    def test_yearly_trends_aggregation(self):
        """Test yearly trends produces correct aggregations."""
        test_data = [
            ("s1", "sk_s1", "Show1", "TV Show", 2020, True),
            ("s2", "sk_s2", "Show2", "TV Show", 2020, True),
            ("m1", "sk_m1", "Movie1", "Movie", 2020, True),
            ("m2", "sk_m2", "Movie2", "Movie", 2021, True)
        ]
        
        titles_df = self._create_mock_titles_df(data=test_data)
        titles_df.createOrReplaceTempView("dim_titles_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        gold.create_gold_yearly_content_trends()
        
        result = self.spark.table("test_yearly_content_trends_gold")
        
        # Should have 3 groups: (2020, TV Show), (2020, Movie), (2021, Movie)
        self.assertEqual(result.count(), 3, "Should have 3 year-type combinations")
        
        # Check 2020 TV Show count
        count_2020_tv = result.filter(
            (col("release_year") == 2020) & (col("type") == "TV Show")
        ).select("total_title").first()[0]
        self.assertEqual(count_2020_tv, 2, "2020 should have 2 TV Shows")

    def test_null_values_in_data(self):
        """Test handling of null values in titles."""
        test_data = [
            ("s1", "sk_s1", "Show1", "TV Show", None, True),  # Null release_year
            ("s2", "sk_s2", None, "Movie", 2020, True),       # Null title
            ("s3", "sk_s3", "Show3", "TV Show", 2020, True)   # Valid
        ]
        
        titles_df = self._create_mock_titles_df(data=test_data)
        titles_df.createOrReplaceTempView("dim_titles_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        # Should handle nulls gracefully
        gold.create_gold_yearly_content_trends()
        
        result = self.spark.table("test_yearly_content_trends_gold")
        # At least one valid record should be processed
        self.assertGreaterEqual(result.count(), 1, "Should process valid records despite nulls")

    def test_duplicate_show_ids(self):
        """Test SCD Type 2 version-aware join: bridge relationships for BOTH inactive V1 and active V2.
        
        This test proves Gold joins on title_version_sk (not show_id) by including bridge
        rows for both the inactive V1 and active V2 of the same show_id. If Gold incorrectly
        joined on show_id, V1's relationships would leak into the result.
        """
        # Dimension: s1 has two versions (V1 inactive, V2 active), s2 is a separate title
        test_data = [
            ("s1", "sk_s1_v1", "Show1_v1", "TV Show", 2020, False),  # Old version (inactive)
            ("s1", "sk_s1_v2", "Show1_v2", "TV Show", 2020, True),   # Current version (active)
            ("s2", "sk_s2", "Show2", "Movie", 2021, True)
        ]
        
        titles_df = self._create_mock_titles_df(data=test_data)
        
        # Cast dimension: 3 distinct cast members
        cast_data = [
            ("cast_A", "Millie Bobby Brown"),
            ("cast_B", "Winona Ryder"),
            ("cast_C", "Claire Foy")
        ]
        cast_df = self._create_mock_cast_df(data=cast_data)
        
        # Bridge table contains relationships for BOTH V1 and V2 of s1.
        # V1 (inactive): cast_A, cast_B
        # V2 (active):   cast_A, cast_C
        # If Gold joined on show_id instead of title_version_sk,
        # cast_B (V1-only) would incorrectly appear in the result.
        bridge_data = [
            ("s1", "sk_s1_v1", "cast_A"),   # V1 relationship
            ("s1", "sk_s1_v1", "cast_B"),   # V1 relationship (must NOT appear in Gold)
            ("s1", "sk_s1_v2", "cast_A"),   # V2 relationship
            ("s1", "sk_s1_v2", "cast_C"),   # V2 relationship
            ("s2", "sk_s2", "cast_B")       # s2 relationship
        ]
        bridge_df = self._create_mock_bridge_df(data=bridge_data)
        
        titles_df.createOrReplaceTempView("dim_titles_silver")
        cast_df.createOrReplaceTempView("dim_cast_silver")
        bridge_df.createOrReplaceTempView("bridge_title_cast_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        gold.create_gold_content_by_cast()
        
        result = self.spark.table("test_content_by_cast_gold")
        
        # --- Assertions on s1 ---
        s1_results = result.filter(col("show_id") == "s1").collect()
        
        # Exactly 2 rows for s1 (cast_A and cast_C from V2 only)
        self.assertEqual(len(s1_results), 2,
                         "s1 should have exactly 2 Gold rows (cast_A and cast_C from active V2)")
        
        # All s1 results must be from V2 (active version)
        for row in s1_results:
            self.assertEqual(row["title_version_sk"], "sk_s1_v2",
                             "All s1 rows must join to active V2 title_version_sk")
            self.assertEqual(row["title"], "Show1_v2",
                             "All s1 rows must use the current version title")
        
        # cast_IDs in s1 results must be cast_A and cast_C (from V2)
        s1_cast_ids = sorted([row["cast_id"] for row in s1_results])
        self.assertEqual(s1_cast_ids, ["cast_A", "cast_C"],
                         "s1 Gold cast_ids must be exactly cast_A and cast_C from V2")
        
        # cast_B must NOT appear in s1 results (it's a V1-only relationship)
        s1_cast_names = [row["cast_name"] for row in s1_results]
        self.assertNotIn("Winona Ryder", s1_cast_names,
                         "cast_B (Winona Ryder) is V1-only and must NOT appear in Gold for s1")
        
        # --- Assertions on s2 ---
        s2_results = result.filter(col("show_id") == "s2").collect()
        self.assertEqual(len(s2_results), 1, "s2 should have exactly 1 Gold row")
        self.assertEqual(s2_results[0]["cast_id"], "cast_B",
                         "s2 should join to cast_B")
        
        # --- Total row count ---
        self.assertEqual(result.count(), 3,
                         "Total Gold rows: 2 (s1 V2) + 1 (s2) = 3")

    def test_write_mode_overwrite(self):
        """Test that overwrite mode replaces existing data."""
        titles_df = self._create_mock_titles_df()
        cast_df = self._create_mock_cast_df()
        bridge_df = self._create_mock_bridge_df()
        
        titles_df.createOrReplaceTempView("dim_titles_silver")
        cast_df.createOrReplaceTempView("dim_cast_silver")
        bridge_df.createOrReplaceTempView("bridge_title_cast_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        # First run
        gold.create_gold_content_by_cast()
        first_count = self.spark.table("test_content_by_cast_gold").count()
        
        # Second run with different data (should overwrite)
        new_data = [("s99", "sk_s99", "New Show", "TV Show", 2025, True)]
        new_titles = self._create_mock_titles_df(data=new_data)
        new_titles.createOrReplaceTempView("dim_titles_silver")
        
        gold.create_gold_content_by_cast()
        second_count = self.spark.table("test_content_by_cast_gold").count()
        
        # Count should be 0 because new titles don't have matching bridge entries
        self.assertNotEqual(first_count, second_count, "Overwrite mode should replace data")



if __name__ == '__main__':
    # Run tests with verbosity (exit=False prevents SystemExit in Databricks)
    unittest.main(argv=['first-arg-is-ignored'], exit=False, verbosity=2)
