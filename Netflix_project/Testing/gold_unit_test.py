import unittest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import col, count
import sys
import os

# Add parent directory to path to import from framework notebook
sys.path.insert(0, '/Workspace/Users/pongsakronk009@hotmail.com/Databricks-for-Data-Engineers-Bootcamp2/Netflix_project')

# Import GoldLayer class definition
from dataclasses import dataclass


@dataclass
class GoldLayer:
    """Gold Layer for creating business-ready aggregation tables."""
    table_name: str
    keys: list[str]
    write_mode: str
    spark: SparkSession = None

    def __post_init__(self):
        self.gold_table_content_by_cast = f"{self.table_name}_content_by_cast_gold"
        self.gold_yearly_content_trends = f"{self.table_name}_yearly_content_trends_gold"
        
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()

    def create_gold_content_by_cast(self) -> None:
        """Create flattened view of content by cast."""
        title_active_df = (
            self.spark.table("dim_titles_silver")
            .filter(col("active_flag") == True)
        )
        
        bridge_cast_df = self.spark.table("bridge_title_cast_silver")
        cast_df = self.spark.table("dim_cast_silver")

        flattened_cast_df = (
            title_active_df.alias("t")
            .join(bridge_cast_df.alias("b"), col("t.show_id") == col("b.show_id"), "inner")
            .join(cast_df.alias("c"), col("b.cast_id") == col("c.cast_id"), "inner")
            .drop(col("b.cast_id"))
            .drop(col("t.show_id"))
        )

        flattened_cast_df.write.format("delta").mode(self.write_mode).saveAsTable(
            self.gold_table_content_by_cast
        )
    
    def create_gold_yearly_content_trends(self) -> None:
        """Create yearly content trends aggregation."""
        title_df = (
            self.spark.table("dim_titles_silver")
            .filter(col("active_flag") == True)
        )
        
        summary_trends_df = (
            title_df
            .groupBy(col("release_year"), col("type"))
            .agg(count("show_id").alias("total_title"))
            .orderBy(col("release_year").desc(), col("type"))
        )
        
        summary_trends_df.write.format("delta").mode(self.write_mode).saveAsTable(
            self.gold_yearly_content_trends
        )

    def run_gold_pipeline(self) -> None:
        """Run all gold pipeline transformations."""
        self.create_gold_content_by_cast()
        self.create_gold_yearly_content_trends()


class TestGoldLayerWithMocks(unittest.TestCase):
    """Test GoldLayer with mocked Spark tables to avoid using real data."""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session once for all tests."""
        # Use existing Spark session (required for Spark Connect/Serverless)
        cls.spark = SparkSession.getActiveSession()
        if cls.spark is None:
            cls.spark = SparkSession.builder.appName("GoldLayerUnitTests").getOrCreate()
    
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
            StructField("title", StringType(), True),
            StructField("type", StringType(), True),
            StructField("release_year", IntegerType(), True),
            StructField("active_flag", BooleanType(), True)
        ])
        
        if data is None:
            data = [
                ("s1", "Stranger Things", "TV Show", 2016, True),
                ("s2", "The Crown", "TV Show", 2016, True),
                ("m1", "Bird Box", "Movie", 2018, True)
            ]
        
        return self.spark.createDataFrame(data, schema)
    
    def _create_mock_cast_df(self, data=None):
        """Helper to create mock cast DataFrame."""
        schema = StructType([
            StructField("cast_id", IntegerType(), True),
            StructField("cast_name", StringType(), True)
        ])
        
        if data is None:
            data = [
                (1, "Millie Bobby Brown"),
                (2, "Winona Ryder"),
                (3, "Claire Foy")
            ]
        
        return self.spark.createDataFrame(data, schema)
    
    def _create_mock_bridge_df(self, data=None):
        """Helper to create mock bridge DataFrame."""
        schema = StructType([
            StructField("show_id", StringType(), True),
            StructField("cast_id", IntegerType(), True)
        ])
        
        if data is None:
            data = [
                ("s1", 1),
                ("s1", 2),
                ("s2", 3)
            ]
        
        return self.spark.createDataFrame(data, schema)

    def test_initialization(self):
        """Test GoldLayer initializes correctly."""
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark
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
            spark=self.spark
        )
        
        # Should not raise error, just create empty table
        gold.create_gold_content_by_cast()
        
        result = self.spark.table("test_content_by_cast_gold")
        self.assertEqual(result.count(), 0, "Empty input should produce empty output table")

    def test_no_active_records(self):
        """Test when all titles are inactive (active_flag=False)."""
        # Create data with all inactive records
        inactive_data = [
            ("s1", "Old Show", "TV Show", 2010, False),
            ("s2", "Old Movie", "Movie", 2011, False)
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
            spark=self.spark
        )
        
        gold.create_gold_content_by_cast()
        
        result = self.spark.table("test_content_by_cast_gold")
        self.assertEqual(result.count(), 0, "Inactive records should be filtered out")

    def test_missing_cast_in_bridge(self):
        """Test when bridge table references non-existent cast."""
        titles_df = self._create_mock_titles_df()
        cast_df = self._create_mock_cast_df()
        
        # Bridge references cast_id=999 which doesn't exist in cast table
        invalid_bridge_data = [
            ("s1", 1),
            ("s1", 999)  # Invalid cast_id
        ]
        bridge_df = self._create_mock_bridge_df(data=invalid_bridge_data)
        
        titles_df.createOrReplaceTempView("dim_titles_silver")
        cast_df.createOrReplaceTempView("dim_cast_silver")
        bridge_df.createOrReplaceTempView("bridge_title_cast_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark
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
            spark=self.spark
        )
        
        gold.create_gold_yearly_content_trends()
        
        result = self.spark.table("test_yearly_content_trends_gold")
        self.assertEqual(result.count(), 0, "Empty input should produce empty trends table")

    def test_yearly_trends_aggregation(self):
        """Test yearly trends produces correct aggregations."""
        test_data = [
            ("s1", "Show1", "TV Show", 2020, True),
            ("s2", "Show2", "TV Show", 2020, True),
            ("m1", "Movie1", "Movie", 2020, True),
            ("m2", "Movie2", "Movie", 2021, True)
        ]
        
        titles_df = self._create_mock_titles_df(data=test_data)
        titles_df.createOrReplaceTempView("dim_titles_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark
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
            ("s1", "Show1", "TV Show", None, True),  # Null release_year
            ("s2", None, "Movie", 2020, True),       # Null title
            ("s3", "Show3", "TV Show", 2020, True)   # Valid
        ]
        
        titles_df = self._create_mock_titles_df(data=test_data)
        titles_df.createOrReplaceTempView("dim_titles_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark
        )
        
        # Should handle nulls gracefully
        gold.create_gold_yearly_content_trends()
        
        result = self.spark.table("test_yearly_content_trends_gold")
        # At least one valid record should be processed
        self.assertGreaterEqual(result.count(), 1, "Should process valid records despite nulls")

    def test_duplicate_show_ids(self):
        """Test handling of duplicate show_ids (SCD Type 2 scenario)."""
        test_data = [
            ("s1", "Show1_v1", "TV Show", 2020, False),  # Old version
            ("s1", "Show1_v2", "TV Show", 2020, True),   # Current version
            ("s2", "Show2", "Movie", 2021, True)
        ]
        
        titles_df = self._create_mock_titles_df(data=test_data)
        cast_df = self._create_mock_cast_df()
        bridge_df = self._create_mock_bridge_df(data=[("s1", 1), ("s2", 2)])
        
        titles_df.createOrReplaceTempView("dim_titles_silver")
        cast_df.createOrReplaceTempView("dim_cast_silver")
        bridge_df.createOrReplaceTempView("bridge_title_cast_silver")
        
        gold = GoldLayer(
            table_name="test",
            keys=["show_id"],
            write_mode="overwrite",
            spark=self.spark
        )
        
        gold.create_gold_content_by_cast()
        
        result = self.spark.table("test_content_by_cast_gold")
        
        # Should only include active version (s1_v2)
        s1_results = result.filter(col("title").contains("Show1")).collect()
        self.assertEqual(len(s1_results), 1, "Should only have one active version of s1")
        self.assertEqual(s1_results[0]["title"], "Show1_v2", "Should use current version")

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
            spark=self.spark
        )
        
        # First run
        gold.create_gold_content_by_cast()
        first_count = self.spark.table("test_content_by_cast_gold").count()
        
        # Second run with different data (should overwrite)
        new_data = [("s99", "New Show", "TV Show", 2025, True)]
        new_titles = self._create_mock_titles_df(data=new_data)
        new_titles.createOrReplaceTempView("dim_titles_silver")
        
        gold.create_gold_content_by_cast()
        second_count = self.spark.table("test_content_by_cast_gold").count()
        
        # Count should be 0 because new titles don't have matching bridge entries
        self.assertNotEqual(first_count, second_count, "Overwrite mode should replace data")


if __name__ == '__main__':
    # Run tests with verbosity
    unittest.main(verbosity=2)
