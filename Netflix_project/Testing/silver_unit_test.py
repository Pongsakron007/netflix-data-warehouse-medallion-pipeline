import unittest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType, DateType
from pyspark.sql.functions import col, count, collect_list, flatten, trim, coalesce, lit, expr, array
from pyspark.sql.functions import current_date, current_timestamp, sha2, concat_ws, explode, split, initcap
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql.window import Window
import sys
import os
# 1. Add import to silver_unit_test.py
from delta import configure_spark_with_delta_pip

# Import logic that works in GitHub Runner, Local, and Databricks
try:
    from unified_fw.fw import SilverLayer
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

    from unified_fw.fw import SilverLayer

class TestSilverLayerWithMocks(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session once for all tests."""
        cls.spark = SparkSession.getActiveSession()
        if cls.spark is None:
            builder = (
                SparkSession.builder
                .appName("SilverLayerUnitTests")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
            cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after all tests."""
        pass
    
    def setUp(self):
        """Set up test fixtures before each test."""
        self.spark.sql("DROP TABLE IF EXISTS test_silver")
        self.spark.sql("DROP TABLE IF EXISTS test_bronze_bad_record")
        
        # Define test schema for Netflix data
        self.test_schema = {
            "show_id": "string",
            "type": "string",
            "title": "string",
            "release_year": "int",
            "rating": "string"
        }
        
        self.test_keys = ["show_id"]
    
    def tearDown(self):
        """Clean up after each test."""
        self.spark.sql("DROP TABLE IF EXISTS test_silver")
        self.spark.sql("DROP TABLE IF EXISTS test_bronze_bad_record")
    
    def _create_mock_bronze_df(self, data=None):
        """Helper to create mock bronze DataFrame."""
        schema = StructType([
            StructField("show_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("release_year", StringType(), True),  # String before type conversion
            StructField("rating", StringType(), True),
            StructField("_sk", IntegerType(), True)
        ])
        
        if data is None:
            data = [
                ("s1", "TV Show", "Stranger Things", "2016", "TV-14", 1),
                ("s2", "Movie", "Bird Box", "2018", "R", 2),
                ("s3", "TV Show", "The Crown", "2016", "TV-MA", 3)
            ]
        
        return self.spark.createDataFrame(data, schema)

    def test_initialization(self):
        """Test SilverLayer initializes correctly."""
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        self.assertEqual(silver.table_name, "test")
        self.assertEqual(silver.bronze_table_name, "test_bronze")
        self.assertEqual(silver.silver_table_name, "test_silver")
        self.assertEqual(silver.bad_record_table_name, "test_bronze_bad_record")
        self.assertEqual(silver.keys, ["show_id"])
        self.assertEqual(len(silver.data_col), 5)
        self.assertIsNotNone(silver.spark)

    def test_trim_data(self):
        """Test trimming whitespace from string columns."""
        # Create data with leading/trailing spaces
        data_with_spaces = [
            ("s1", " TV Show ", "  Stranger Things  ", "2016", "TV-14", 1),
            ("s2", "Movie", "Bird Box   ", "2018", "  R", 2)
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=data_with_spaces)
        trimmed_df = silver.trim_data(df)
        
        # Check that strings are trimmed
        first_row = trimmed_df.first()
        self.assertEqual(first_row["type"], "TV Show")
        self.assertEqual(first_row["title"], "Stranger Things")
        self.assertEqual(first_row["rating"], "TV-14")

    def test_change_data_type_valid(self):
        """Test data type conversion with valid data."""
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df()
        converted_df = silver.change_data_type(df)
        
        # Check that release_year is now int
        schema_dict = {field.name: field.dataType.simpleString() for field in converted_df.schema.fields}
        self.assertEqual(schema_dict["release_year"], "int")
        
        # Check values are correct
        first_row = converted_df.filter(col("show_id") == "s1").first()
        self.assertEqual(first_row["release_year"], 2016)

    def test_change_data_type_invalid(self):
        """Test data type conversion with invalid data returns NULL."""
        # Create data with invalid year
        invalid_data = [
            ("s1", "TV Show", "Show1", "invalid_year", "TV-14", 1),
            ("s2", "Movie", "Movie1", "2020", "R", 2)
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=invalid_data)
        converted_df = silver.change_data_type(df)
        
        # Invalid conversion should result in NULL
        invalid_row = converted_df.filter(col("show_id") == "s1").first()
        self.assertIsNone(invalid_row["release_year"])
        
        # Valid conversion should work
        valid_row = converted_df.filter(col("show_id") == "s2").first()
        self.assertEqual(valid_row["release_year"], 2020)

    def test_get_invalid_record(self):
        """Test identifying invalid records (NULL after type conversion)."""
        # Create data with invalid int
        invalid_data = [
            ("s1", "TV Show", "Show1", "abc123", "TV-14", 1),  # Invalid year
            ("s2", "Movie", "Movie1", "2020", "R", 2)  # Valid
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=invalid_data)
        converted_df = silver.change_data_type(df)
        invalid_df = silver.get_invalid_record(converted_df)
        
        # Should have 1 invalid record
        self.assertEqual(invalid_df.count(), 1)
        
        # Check the reason
        invalid_row = invalid_df.first()
        self.assertEqual(invalid_row["show_id"], "s1")
        self.assertIn("_is_release_year_invalid", invalid_row["reason"])

    def test_get_key_null_record(self):
        """Test identifying records with null keys."""
        # Create data with null key
        null_key_data = [
            (None, "TV Show", "Show1", "2020", "TV-14", 1),  # Null show_id
            ("s2", "Movie", "Movie1", "2021", "R", 2)  # Valid
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=null_key_data)
        converted_df = silver.change_data_type(df)
        key_null_df = silver.get_key_null_record(converted_df)
        
        # Should have 1 null key record
        self.assertEqual(key_null_df.count(), 1)
        
        # Check the reason
        null_row = key_null_df.first()
        self.assertIsNone(null_row["show_id"])
        self.assertIn("_is_show_id_null", null_row["reason"])

    def test_get_invalid_show_id_record(self):
        """Test identifying records with invalid show_id pattern."""
        # Create data with invalid show_id patterns
        invalid_show_id_data = [
            ("Flying Fortress\"", "Movie", "Documentary", "2020", "TV-14", 1),  # Invalid pattern
            (" and probably will.\"", "TV Show", "Show1", "2021", "TV-MA", 2),  # Invalid pattern
            ("s123", "Movie", "Movie1", "2022", "R", 3),  # Valid pattern
            ("s1", "TV Show", "Show2", "2023", "TV-PG", 4),  # Valid pattern
            ("show123", "Movie", "Movie2", "2024", "PG-13", 5)  # Invalid (no 's' prefix)
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=invalid_show_id_data)
        converted_df = silver.change_data_type(df)
        invalid_show_id_df = silver.get_invalid_show_id_record(converted_df)
        
        # Should have 3 invalid show_id patterns
        self.assertEqual(invalid_show_id_df.count(), 3)
        
        # Check the reasons
        invalid_ids = [row["show_id"] for row in invalid_show_id_df.collect()]
        self.assertIn("Flying Fortress\"", invalid_ids)
        self.assertIn(" and probably will.\"", invalid_ids)
        self.assertIn("show123", invalid_ids)
        self.assertNotIn("s123", invalid_ids)
        self.assertNotIn("s1", invalid_ids)
        
        # All should have _is_show_id_invalid reason
        for row in invalid_show_id_df.collect():
            self.assertIn("_is_show_id_invalid", row["reason"])

    def test_get_dup_record_row_duplication(self):
        """Test identifying exact duplicate rows."""
        # Create data with exact duplicates
        dup_data = [
            ("s1", "TV Show", "Show1", "2020", "TV-14", 1),
            ("s1", "TV Show", "Show1", "2020", "TV-14", 2),  # Exact duplicate
            ("s2", "Movie", "Movie1", "2021", "R", 3)
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=dup_data)
        converted_df = silver.change_data_type(df)
        key_null_df = silver.get_key_null_record(converted_df)
        dup_df = silver.get_dup_record(converted_df, key_null_df)
        
        # Should identify 1 duplicate (the second occurrence)
        self.assertGreaterEqual(dup_df.count(), 1)
        
        # Check reason contains row duplication
        reasons = dup_df.select("reason").collect()
        found_row_dup = False
        for row in reasons:
            if "_row_duplication" in row["reason"]:
                found_row_dup = True
                break
        self.assertTrue(found_row_dup)

    def test_get_dup_record_key_duplication(self):
        """Test identifying key duplicates (same key, different values)."""
        # Create data with same key but different values
        key_dup_data = [
            ("s1", "TV Show", "Show1_v1", "2020", "TV-14", 1),
            ("s1", "TV Show", "Show1_v2", "2021", "TV-MA", 2),  # Same key, different data
            ("s1", "TV Show", "Show1_v3", "2022", "TV-MA", 4),  # Same key, different data
            ("s2", "Movie", "Movie1", "2021", "R", 3)
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=key_dup_data)
        converted_df = silver.change_data_type(df)
        key_null_df = silver.get_key_null_record(converted_df)
        dup_df = silver.get_dup_record(converted_df, key_null_df)
        
        # Should identify 2 key duplicates
        self.assertEqual(dup_df.count(), 3)
        
        # Both should have key_duplicate reason
        reasons = dup_df.select("reason").collect()
        for row in reasons:
            self.assertIn("_key_duplicate", row["reason"])

    def test_get_all_bad_record(self):
        """Test combining all bad records."""
        # Create data with multiple issues
        mixed_data = [
            (None, "TV Show", "Show1", "2020", "TV-14", 1),  # Null key
            ("s2", "Movie", "Movie2", "invalid", "R", 2),  # Invalid year
            ("s3", "TV Show", "Show3", "2020", "TV-MA", 3),
            ("s3", "TV Show", "Show3", "2020", "TV-MA", 4)  # Duplicate
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=mixed_data)
        converted_df = silver.change_data_type(df)
        invalid_df = silver.get_invalid_record(converted_df)
        key_null_df = silver.get_key_null_record(converted_df)
        invalid_show_id_df = silver.get_invalid_show_id_record(converted_df)
        dup_df = silver.get_dup_record(converted_df, key_null_df)
        all_bad_df = silver.get_all_bad_record(invalid_df, key_null_df, invalid_show_id_df, dup_df)
        
        # Should have 3 bad records: null key (s_sk=1), invalid year (s_sk=2), duplicate (s_sk=4)
        # Note: s_sk=3 is the first occurrence of s3, which is not considered bad
        self.assertEqual(all_bad_df.count(), 3)

    def test_get_final_result(self):
        """Test getting only good records."""
        # Create data with some bad records
        mixed_data = [
            (None, "TV Show", "Show1", "2020", "TV-14", 1),  # Bad: null key
            ("s2", "Movie", "Movie2", "2021", "R", 2),  # Good
            ("s3", "TV Show", "Show3", "2022", "TV-MA", 3)  # Good
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=mixed_data)
        converted_df = silver.change_data_type(df)
        invalid_df = silver.get_invalid_record(converted_df)
        key_null_df = silver.get_key_null_record(converted_df)
        invalid_show_id_df = silver.get_invalid_show_id_record(converted_df)
        dup_df = silver.get_dup_record(converted_df, key_null_df)
        all_bad_df = silver.get_all_bad_record(invalid_df, key_null_df, invalid_show_id_df, dup_df)
        final_df = silver.get_final_result(converted_df, all_bad_df)
        
        # Should have 2 good records
        self.assertEqual(final_df.count(), 2)
        
        # Should have load_dt and load_dttm columns
        self.assertIn("load_dt", final_df.columns)
        self.assertIn("load_dttm", final_df.columns)
        
        # Good records should be s2 and s3
        show_ids = [row["show_id"] for row in final_df.collect()]
        self.assertIn("s2", show_ids)
        self.assertIn("s3", show_ids)
        self.assertNotIn(None, show_ids)

    def test_empty_bronze_data(self):
        """Test behavior with empty bronze data."""
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        # Create empty dataframe
        empty_df = self._create_mock_bronze_df(data=[])
        
        converted_df = silver.change_data_type(empty_df)
        invalid_df = silver.get_invalid_record(converted_df)
        key_null_df = silver.get_key_null_record(converted_df)
        invalid_show_id_df = silver.get_invalid_show_id_record(converted_df)
        dup_df = silver.get_dup_record(converted_df, key_null_df)
        all_bad_df = silver.get_all_bad_record(invalid_df, key_null_df, invalid_show_id_df, dup_df)
        final_df = silver.get_final_result(converted_df, all_bad_df)
        
        # All should be empty
        self.assertEqual(converted_df.count(), 0)
        self.assertEqual(invalid_df.count(), 0)
        self.assertEqual(key_null_df.count(), 0)
        self.assertEqual(dup_df.count(), 0)
        self.assertEqual(all_bad_df.count(), 0)
        self.assertEqual(final_df.count(), 0)

    def test_all_valid_data(self):
        """Test with 100% valid data."""
        valid_data = [
            ("s1", "TV Show", "Show1", "2020", "TV-14", 1),
            ("s2", "Movie", "Movie1", "2021", "R", 2),
            ("s3", "TV Show", "Show3", "2022", "TV-MA", 3),
            ("s4", "Movie", "Movie2", "2023", "PG-13", 4), # add another valid record
            ("s5", "TV Show", "Show5", "2024", "TV-14", 5)
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=valid_data)
        converted_df = silver.change_data_type(df)
        invalid_df = silver.get_invalid_record(converted_df)
        key_null_df = silver.get_key_null_record(converted_df)
        invalid_show_id_df = silver.get_invalid_show_id_record(converted_df)
        dup_df = silver.get_dup_record(converted_df, key_null_df)
        all_bad_df = silver.get_all_bad_record(invalid_df, key_null_df, invalid_show_id_df, dup_df)
        final_df = silver.get_final_result(converted_df, all_bad_df)
        
        # No bad records
        self.assertEqual(invalid_df.count(), 0)
        self.assertEqual(key_null_df.count(), 0)
        self.assertEqual(dup_df.count(), 0)
        self.assertEqual(all_bad_df.count(), 0)
        
        # All records should be good
        self.assertEqual(final_df.count(), 5)

    def test_all_bad_data(self):
        """Test with 100% bad data including invalid show_id."""
        all_bad_data = [
            (None, "TV Show", "Show1", "invalid", "TV-14", 1),  # Null key + invalid year
            (None, "Movie", "Movie1", "abc", "R", 2),  # Null key + invalid year
            ("bad_id", "TV Show", "Show2", "2020", "TV-MA", 3),  # Invalid show_id pattern
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=all_bad_data)
        converted_df = silver.change_data_type(df)
        invalid_df = silver.get_invalid_record(converted_df)
        key_null_df = silver.get_key_null_record(converted_df)
        invalid_show_id_df = silver.get_invalid_show_id_record(converted_df)
        dup_df = silver.get_dup_record(converted_df, key_null_df)
        all_bad_df = silver.get_all_bad_record(invalid_df, key_null_df, invalid_show_id_df, dup_df)
        final_df = silver.get_final_result(converted_df, all_bad_df)
        
        # All records should be bad (2 null keys, 2 invalid years, 1 invalid show_id)
        self.assertGreaterEqual(all_bad_df.count(), 3)
        
        # No good records
        self.assertEqual(final_df.count(), 0)

    def test_multi_reason_bad_record(self):
        """Test record with multiple validation failures (multi-reason bad record)."""
        # Create data with multiple issues on same record
        multi_reason_data = [
            ("invalid_id", "TV Show", "Show1", "not_a_year", "TV-14", 1),  # Invalid show_id + invalid year
            ("invalid_ids", "TV Show", "Show1", "not_a_years", "TV-14", 3),  # Invalid show_id + invalid year
            ("s123", "Movie", "Movie1", "2021", "R", 2)  # Good record
        ]
        
        silver = SilverLayer(
            table_name="test",
            schema_detail=self.test_schema,
            keys=self.test_keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""  # Empty prefix for tests
        )
        
        df = self._create_mock_bronze_df(data=multi_reason_data)
        converted_df = silver.change_data_type(df)
        invalid_df = silver.get_invalid_record(converted_df)
        key_null_df = silver.get_key_null_record(converted_df)
        invalid_show_id_df = silver.get_invalid_show_id_record(converted_df)
        dup_df = silver.get_dup_record(converted_df, key_null_df)
        all_bad_df = silver.get_all_bad_record(invalid_df, key_null_df, invalid_show_id_df, dup_df)
        final_df = silver.get_final_result(converted_df, all_bad_df)
        
        # Should have 1 bad record with multiple reasons
        self.assertEqual(all_bad_df.count(), 2)
        
        # The bad record should have BOTH reasons
        bad_row = all_bad_df.orderBy("show_id").first()
        self.assertEqual(bad_row["show_id"], "invalid_id")
        self.assertEqual(len(bad_row["reason"]), 2)  # Two reasons
        self.assertIn("_is_show_id_invalid", bad_row["reason"])
        self.assertIn("_is_release_year_invalid", bad_row["reason"])
        
        # Only 1 good record should remain
        self.assertEqual(final_df.count(), 1)
        self.assertEqual(final_df.first()["show_id"], "s123")


class TestHashAndVersionKeys(unittest.TestCase):
    """Tests for relationship-aware hash_value, title_version_sk determinism, and bridge/Gold logic."""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session once for all tests."""
        cls.spark = SparkSession.getActiveSession()
        if cls.spark is None:
            builder = (
                SparkSession.builder
                .appName("HashAndVersionKeyTests")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            )
            cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()

    def setUp(self):
        """Set up full production schema for hash/version tests."""
        self.full_schema = {
            "show_id": "string",
            "type": "string",
            "title": "string",
            "director": "string",
            "cast": "string",
            "country": "string",
            "date_added": "date",
            "release_year": "integer",
            "rating": "string",
            "duration": "string",
            "listed_in": "string",
            "description": "string"
        }
        self.keys = ["show_id"]
        self.silver = SilverLayer(
            table_name="test",
            schema_detail=self.full_schema,
            keys=self.keys,
            write_mode="overwrite",
            spark=self.spark,
            table_prefix=""
        )

    def _create_test_df(self, rows):
        """Create a DataFrame with all 12 production columns + _sk as LongType."""
        schema = StructType([
            StructField("show_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("director", StringType(), True),
            StructField("cast", StringType(), True),
            StructField("country", StringType(), True),
            StructField("date_added", StringType(), True),
            StructField("release_year", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("listed_in", StringType(), True),
            StructField("description", StringType(), True),
            StructField("_sk", LongType(), True)
        ])
        return self.spark.createDataFrame(rows, schema)

    def _get_hash_value(self, df):
        """Helper: extract hash_value for the first row."""
        result = self.silver.get_hash_key_value(df)
        return result.select("hash_value").first()["hash_value"]

    def _get_title_version_sk(self, df):
        """Helper: extract title_version_sk for the first row."""
        result = self.silver.get_hash_key_value(df)
        return result.select("title_version_sk").first()["title_version_sk"]

    def test_relationship_columns_in_hash_value(self):
        """cast/director/country/listed_in changes must produce different hash_value."""
        row_v1 = ("s1", "Movie", "Test", "Dir A", "Tom, John", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "Desc", 1)
        row_v2 = ("s1", "Movie", "Test", "Dir A", "Tom, Sarah", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "Desc", 2)
        self.assertNotEqual(
            self._get_hash_value(self._create_test_df([row_v1])),
            self._get_hash_value(self._create_test_df([row_v2]))
        )

    def test_cast_ordering_no_change(self):
        """Reordering cast members should not change hash_value."""
        row_a = ("s1", "Movie", "T", "D", "Tom, John, Mike", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        row_b = ("s1", "Movie", "T", "D", "Mike, Tom, John", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)
        self.assertEqual(
            self._get_hash_value(self._create_test_df([row_a])),
            self._get_hash_value(self._create_test_df([row_b]))
        )

    def test_director_ordering_no_change(self):
        """Reordering directors should not change hash_value."""
        row_a = ("s1", "Movie", "T", "Tom, John, Mike", "Cast", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        row_b = ("s1", "Movie", "T", "Mike, Tom, John", "Cast", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)
        self.assertEqual(
            self._get_hash_value(self._create_test_df([row_a])),
            self._get_hash_value(self._create_test_df([row_b]))
        )

    def test_country_ordering_no_change(self):
        """Reordering countries should not change hash_value."""
        row_a = ("s1", "Movie", "T", "D", "Cast", "USA, UK, France",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        row_b = ("s1", "Movie", "T", "D", "Cast", "France, USA, UK",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)
        self.assertEqual(
            self._get_hash_value(self._create_test_df([row_a])),
            self._get_hash_value(self._create_test_df([row_b]))
        )

    def test_listed_in_ordering_no_change(self):
        """Reordering listed_in categories should not change hash_value."""
        row_a = ("s1", "Movie", "T", "D", "Cast", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama, Comedy, Action", "X", 1)
        row_b = ("s1", "Movie", "T", "D", "Cast", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Action, Drama, Comedy", "X", 2)
        self.assertEqual(
            self._get_hash_value(self._create_test_df([row_a])),
            self._get_hash_value(self._create_test_df([row_b]))
        )

    def test_adding_member_changes_hash(self):
        """Adding a relationship member should change hash_value."""
        row_v1 = ("s1", "Movie", "T", "D", "Tom, John", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        row_v2 = ("s1", "Movie", "T", "D", "Tom, John, Mike", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)
        self.assertNotEqual(
            self._get_hash_value(self._create_test_df([row_v1])),
            self._get_hash_value(self._create_test_df([row_v2]))
        )

    def test_removing_member_changes_hash(self):
        """Removing a relationship member should change hash_value."""
        row_v1 = ("s1", "Movie", "T", "D", "Tom, John, Mike", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        row_v2 = ("s1", "Movie", "T", "D", "Tom, John", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)
        self.assertNotEqual(
            self._get_hash_value(self._create_test_df([row_v1])),
            self._get_hash_value(self._create_test_df([row_v2]))
        )

    def test_empty_values_filtered(self):
        """Extra commas producing empty strings should not affect the hash."""
        row_a = ("s1", "Movie", "T", "D", "Tom, John", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        row_b = ("s1", "Movie", "T", "D", "Tom, , John", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)
        self.assertEqual(
            self._get_hash_value(self._create_test_df([row_a])),
            self._get_hash_value(self._create_test_df([row_b]))
        )

    def test_same_set_different_order_same_hash(self):
        """All relationship columns reordered should produce the same hash_value."""
        row_a = ("s1", "Movie", "T", "Dir A, Dir B", "Tom, John", "USA, UK",
                 "January 1, 2020", "2020", "R", "120 min", "Drama, Comedy", "X", 1)
        row_b = ("s1", "Movie", "T", "Dir B, Dir A", "John, Tom", "UK, USA",
                 "January 1, 2020", "2020", "R", "120 min", "Comedy, Drama", "X", 2)
        self.assertEqual(
            self._get_hash_value(self._create_test_df([row_a])),
            self._get_hash_value(self._create_test_df([row_b]))
        )

    def test_title_version_sk_deterministic(self):
        """Same content should always produce the same title_version_sk."""
        row = ("s1", "Movie", "T", "D", "Tom, John", "USA",
               "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        df = self._create_test_df([row])
        sk1 = self._get_title_version_sk(df)
        sk2 = self._get_title_version_sk(df)
        self.assertEqual(sk1, sk2)

    def test_title_version_sk_same_across_different_sk(self):
        """title_version_sk must be the same even when ephemeral _sk differs."""
        row_a = ("s1", "Movie", "T", "D", "Tom, John", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 100)
        row_b = ("s1", "Movie", "T", "D", "Tom, John", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 999)
        self.assertEqual(
            self._get_title_version_sk(self._create_test_df([row_a])),
            self._get_title_version_sk(self._create_test_df([row_b]))
        )

    def test_content_change_different_title_version_sk(self):
        """A scalar content change should produce a different title_version_sk."""
        row_v1 = ("s1", "Movie", "Title A", "D", "Tom, John", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        row_v2 = ("s1", "Movie", "Title B", "D", "Tom, John", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)
        self.assertNotEqual(
            self._get_title_version_sk(self._create_test_df([row_v1])),
            self._get_title_version_sk(self._create_test_df([row_v2]))
        )

    def test_different_entity_different_sk(self):
        """Different show_ids should produce different title_version_sk."""
        row_a = ("s1", "Movie", "T", "D", "Tom", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        row_b = ("s2", "Movie", "T", "D", "Tom", "USA",
                 "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)
        self.assertNotEqual(
            self._get_title_version_sk(self._create_test_df([row_a])),
            self._get_title_version_sk(self._create_test_df([row_b]))
        )

    def test_relationship_only_change_new_version(self):
        """A relationship-only change (scalars unchanged) should produce a different title_version_sk."""
        row_v1 = ("s1", "Movie", "T", "D", "Tom, John", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        row_v2 = ("s1", "Movie", "T", "D", "Tom, Sarah", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)
        self.assertNotEqual(
            self._get_title_version_sk(self._create_test_df([row_v1])),
            self._get_title_version_sk(self._create_test_df([row_v2]))
        )

    def test_hash_method_preserves_columns(self):
        """get_hash_key_value should NOT drop explode columns or _sk."""
        row = ("s1", "Movie", "T", "D", "Tom", "USA",
               "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        df = self._create_test_df([row])
        result = self.silver.get_hash_key_value(df)
        cols = result.columns
        for c in ["show_id", "type", "title", "director", "cast", "country",
                  "date_added", "release_year", "rating", "duration", "listed_in",
                  "description", "_sk"]:
            self.assertIn(c, cols)
        for c in ["hash_key", "hash_value", "title_version_sk"]:
            self.assertIn(c, cols)

    def test_title_version_sk_formula(self):
        """title_version_sk should equal sha2(concat_ws('||', hash_key, hash_value), 256)."""
        row = ("s1", "Movie", "T", "D", "Tom", "USA",
               "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        df = self._create_test_df([row])
        result = self.silver.get_hash_key_value(df)
        row_data = result.select("hash_key", "hash_value", "title_version_sk").first()
        expected_df = self.spark.createDataFrame(
            [(row_data["hash_key"], row_data["hash_value"])],
            ["hash_key", "hash_value"]
        ).withColumn("expected_sk", sha2(concat_ws("||", col("hash_key"), col("hash_value")), 256))
        expected_sk = expected_df.select("expected_sk").first()["expected_sk"]
        self.assertEqual(row_data["title_version_sk"], expected_sk)

    def test_date_added_in_hash_and_preserved(self):
        """date_added should be in the hash and preserved in the output of get_hash_key_value."""
        row_v1 = ("s1", "Movie", "T", "D", "Tom", "USA",
                  "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)
        row_v2 = ("s1", "Movie", "T", "D", "Tom", "USA",
                  "February 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)
        self.assertNotEqual(
            self._get_hash_value(self._create_test_df([row_v1])),
            self._get_hash_value(self._create_test_df([row_v2]))
        )
        # date_added column should still be in the result
        result = self.silver.get_hash_key_value(self._create_test_df([row_v1]))
        self.assertIn("date_added", result.columns)

    def test_empty_string_vs_whitespace_normalized_same(self):
        """Empty string and whitespace-only relationship values should normalize to the same hash.

        The normalization logic (split -> trim -> filter empty -> initcap -> sort -> join)
        should treat "", "   ", and values with only empty comma-delimited elements consistently.
        """
        base_row = ("s1", "Movie", "T", "Tom, John", "Tom, John", "USA",
                    "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 1)

        # Empty string for all relationship columns
        row_empty = ("s1", "Movie", "T", "", "", "",
                     "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 2)

        # Whitespace-only for all relationship columns
        row_whitespace = ("s1", "Movie", "T", "   ", "   ", "   ",
                          "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 3)

        # Empty comma-delimited elements for all relationship columns
        row_empty_elements = ("s1", "Movie", "T", ", ,", ", ,", ", ,",
                               "January 1, 2020", "2020", "R", "120 min", "Drama", "X", 4)

        hash_empty = self._get_hash_value(self._create_test_df([row_empty]))
        hash_whitespace = self._get_hash_value(self._create_test_df([row_whitespace]))
        hash_empty_elements = self._get_hash_value(self._create_test_df([row_empty_elements]))

        self.assertEqual(hash_empty, hash_whitespace,
                         "Empty string and whitespace-only should normalize to the same hash")
        self.assertEqual(hash_empty, hash_empty_elements,
                         "Empty string and comma-delimited empty elements should normalize to the same hash")

    def test_duplicate_relationship_members_bridge_dedup(self):
        """Duplicate relationship members (e.g., 'Tom, Tom, John') should not produce duplicate bridge rows.

        The bridge transform (_transform_and_explode_bridge) must deduplicate on
        (title_version_sk, relationship_id) so that duplicate members in the source
        do not create duplicate bridge rows.
        """
        # Create a DataFrame with duplicate cast members
        schema = StructType([
            StructField("show_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("director", StringType(), True),
            StructField("cast", StringType(), True),
            StructField("country", StringType(), True),
            StructField("date_added", StringType(), True),
            StructField("release_year", StringType(), True),
            StructField("rating", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("listed_in", StringType(), True),
            StructField("description", StringType(), True),
            StructField("_sk", LongType(), True)
        ])
        row = ("s1", "Movie", "Test", "Dir A", "Tom, Tom, John", "USA",
               "January 1, 2020", "2020", "R", "120 min", "Drama", "Desc", 1)
        df = self.spark.createDataFrame([row], schema)

        # Get hash columns (needed for title_version_sk)
        df_with_hash = self.silver.get_hash_key_value(df)

        # Run the bridge transform
        bridge_df = self.silver._transform_and_explode_bridge(
            df_with_hash, "cast", "cast_name", "cast_id"
        )

        total_rows = bridge_df.count()
        distinct_rows = bridge_df.distinct().count()

        # Should have exactly 2 distinct bridge rows: Tom and John (duplicate Tom deduplicated)
        self.assertEqual(total_rows, 2,
                         f"Bridge should have 2 rows (Tom, John) after dedup, got {total_rows}")
        self.assertEqual(total_rows, distinct_rows,
                         "Bridge should have no duplicate rows")

        # Verify the cast_id values are distinct
        cast_ids = [row["cast_id"] for row in bridge_df.collect()]
        self.assertEqual(len(cast_ids), len(set(cast_ids)),
                         "All cast_id values in bridge should be distinct")


if __name__ == '__main__':
    # Run tests with verbosity
    # Use explicit argv to avoid Databricks notebook path being interpreted as a test module
    unittest.main(argv=['dummy'], verbosity=2, exit=False)
