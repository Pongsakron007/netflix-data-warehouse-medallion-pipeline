from dataclasses import dataclass
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import *
from delta.tables import *
from pyspark.sql.types import *

@dataclass
class BronzeLayer:
    file_path:str
    header:bool
    delimiter:str
    table_name:str
    schema_detail:dict[str, str]
    spark: SparkSession = None
    
    def __post_init__(self) -> None:
        # Extract format - handle both files and folders
        if self.file_path.endswith('/'):
            self.format_type = 'csv' # Folder path - default to csv or make it configurable
        else:
            self.format_type = self.file_path.split('.')[-1] # File path - extract extension
            
        self.target_table_bronze = f'{self.table_name}_bronze'

        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()

    # Alternative method to get variable from config table.
    @classmethod
    def from_config_table(cls, pipeline_name:str) -> "BronzeLayer":
        conf = (spark.table("netflix.config_table")
               .filter(col("pipeline_name") == pipeline_name)
               .select("file_path", "header", "delimiter", "table_name", "schema_detail")
               .first())
        return cls(
            file_path = conf.file_path
            , header = conf.header
            , delimiter = conf.delimiter
            , table_name = conf.table_name
            , schema_detail = conf.schema_detail
        )
    
    # Read data from file
    def read_from_file(self) -> DataFrame:
        df = (
            spark.read.format(self.format_type)
            .option("header", self.header)
            .option("delimiter", self.delimiter)
            .load(self.file_path)
        )
        # return with another metadata columns
        return (
            df
            .withColumns({
                "_load_dt": current_date(),
                "_load_dttm": current_timestamp(),
                "_file_name": col("_metadata.file_name"),
                "_file_path": col("_metadata.file_path"),
                "_file_size": col("_metadata.file_size"),
                "_file_mod": col("_metadata.file_modification_time")
            })
        )
    
    # Load data in bronze table with batch mode.
    def load_to_bronze_table(self, raw_df: DataFrame) -> None:
        # Ensure the bronze table is initialized with correct settings before loading
        self._init_bronze_table() 

        # write data to bronze table
        raw_df.write.mode("append").saveAsTable(self.target_table_bronze)
        print(f"Table {self.target_table_bronze} loaded")

    # Initialize bronze table with correct settings (helper function).
    def _init_bronze_table(self) -> None:
        # created schema for first time and enable CDF to table
        ## 1. First check if table exists or not
        if spark.catalog.tableExists(self.target_table_bronze):
            print(f"Table {self.target_table_bronze} already exists.")
            return # end method immediately
            
        # 2. If table not exitsts. We create table
        else:
            print(f"Table {self.target_table_bronze} does not exist. Initializing...")
            
            # Build schema with data columns (all as strings) + metadata columns
            schema_fields = [
                StructField(col_name, StringType(), True) 
                for col_name in self.schema_detail.keys()
            ]
            
            # Add metadata columns - types must match what read_from_file() produces
            metadata_fields = [
                StructField("_load_dt", DateType(), True),           # current_date() returns DateType
                StructField("_load_dttm", TimestampType(), True),    # current_timestamp() returns TimestampType
                StructField("_file_name", StringType(), True),
                StructField("_file_path", StringType(), True),
                StructField("_file_size", LongType(), True),         # file_size is LongType
                StructField("_file_mod", TimestampType(), True)      # file_modification_time is TimestampType
            ]
            
            bronze_schema = StructType(schema_fields + metadata_fields)
            
            # Create empty DataFrame with defined schema
            empty_df = spark.createDataFrame([], bronze_schema)
            
            # Create table from Schema and enable CDF with Python API
            (
                empty_df.write
                .format("delta")
                .option("delta.enableChangeDataFeed", "true")
                .mode("ignore") # prevent other class creating table before this action
                .saveAsTable(self.target_table_bronze)
            )
            
            print(f" Table {self.target_table_bronze} created successfully with CDF enabled.")
    
    # Define method to ingest data from S3 to Bronze table using Databricks Auto Loader.
    def s3_auto_loader(self, checkpoint_location: str = None) -> None:
        """
        Ingest data from S3 to Bronze table using Databricks Auto Loader.
        Driven by Structured Streaming with Trigger AvailableNow for batch-like cost efficiency.
        Args:
            checkpoint_path (str): S3 location or DBFS path to store streaming checkpoints.
        """

        # Check whether bronze table exists or not
        self._init_bronze_table()

        # Check whether checkpoint location is provided or not
        if checkpoint_location is None:
            checkpoint_location = f"/Volumes/workspace/netflix/checkpoint_dir/{self.table_name}_bronze/"
        
        schema_location = f"/Volumes/workspace/netflix/checkpoint_dir/{self.table_name}_bronze_schema/"

        # For folder paths, extract format from file extension or default to 'csv'
        file_format = 'csv' if self.format_type == '' or '/' in self.format_type else self.format_type # Need to improve this logic in future. It's look nonesense because this logic should handle both s3 path and csv file in appropiated approach.

        # 1. Establish Auto Loader Stream
        auto_loader_netflix = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.schemaEvolutionMode", "rescue") 
            .option("pathGlobFilter", "*.csv")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("header", self.header)
            .option("delimiter", self.delimiter)
            .load(self.file_path)
        )

        # 2. Add metadata columns
        auto_loader_netflix_add_metadata = auto_loader_netflix.withColumns({
            "_load_dt": current_date(),
            "_load_dttm": current_timestamp(),
            "_file_name": col("_metadata.file_name"),
            "_file_path": col("_metadata.file_path"),
            "_file_size": col("_metadata.file_size"),
            "_file_mod": col("_metadata.file_modification_time")
        })


        #  Cover Try-Except 
        try:
            print(" Starting and storing data into Bronze...")
            
            query = (
                auto_loader_netflix_add_metadata
                .writeStream
                .outputMode("append")
                .option("checkpointLocation", checkpoint_location)
                .option("mergeSchema", "true")
                .trigger(availableNow=True)
                .toTable(self.target_table_bronze)
            )
            
            query.awaitTermination()
            print(f" Stream processing complete for {self.target_table_bronze}")
            
        except Exception as e:
            error_msg = str(e)
            # Catch bug Spark Connect Serverless (SPARK-55448)
            if "STATE_CONSISTENCY" in error_msg or "XXSC0" in error_msg or "STATUS_MISMATCH" in error_msg:
                print(" [Serverless Info] Caught Spark Connect Sync Bug (SPARK-55448)")
                print(f"Status: Write data into table {self.target_table_bronze} succesfully")
            else:
                # If error with other error, raise exception to stop stream.
                raise e

@dataclass
class SilverLayer:
    table_name: str
    schema_detail: dict[str, str]
    keys: list[str]
    write_mode: str
    spark: SparkSession = None

    def __post_init__(self) -> None:
        self.bronze_table_name = f"{self.table_name}_bronze"
        self.silver_table_name = f"{self.table_name}_silver"
        self.bad_record_table_name = f"{self.table_name}_bronze_bad_record"
        self.data_col = [col_name for col_name in self.schema_detail.keys()]
        # self.invalid_rule = {"int": "^[0-9]+$", "date": "^\\d{4}-\\d{2}-\\d{2}$"}
        
        if self.spark is None:
            from pyspark.sql import SparkSession
            self.spark = SparkSession.getActiveSession()

    # Alternative method to get variable from config table.
    @classmethod
    def from_config_table(cls, pipeline_name: str) -> "SilverLayer":
        conf = (
            spark.table("workspace.netflix.config_table")
            .filter(col("pipeline_name") == pipeline_name)
            .select(
                "table_name", "schema_detail", "keys", "write_mode"
            )
            .first()
        )
        return cls(
            table_name=conf.table_name,
            schema_detail=conf.schema_detail,
            keys=conf.keys,
            write_mode=conf.write_mode,
        )
    # Helper Method for get invalid reason
    '''
    Helper method to get invalid reason prepare to future explode columns.
    '''
    def _get_reason(self, df: DataFrame) -> DataFrame:
        control_col = [col_name for col_name in df.columns if col_name.startswith("_") and col_name != "_sk"]
        data_col = [col_name for col_name in df.columns if not col_name.startswith("_")]
        or_statement = " OR ".join([col_name for col_name in control_col])
        return (
            df
            .filter(or_statement)
            .melt(
                ids = [*data_col, "_sk"]
                , values = control_col
                , variableColumnName = "reason"
                , valueColumnName = "status"
            )
        .filter(col("status") == True)
        .groupBy(*data_col, "_sk")
        .agg(collect_list("reason").alias("reason"))
        )
    
    # Trim string data logic.
    def trim_data(self, df: DataFrame) -> DataFrame:
        """
        Trim all string columns to remove leading/trailing whitespace.
        Uses select() to avoid performance issues from .withColumn() in a loop.
        """
        # Pre-compute schema to avoid repeated Analyze RPCs
        df_columns = df.columns
        
        trim_exprs = [
            trim(col(col_name)).alias(col_name) if col_type == "string" else col(col_name)
            for col_name, col_type in self.schema_detail.items()
        ]
        # Include _sk if it exists
        if "_sk" in df_columns:
            trim_exprs.append(col("_sk"))
        
        return df.select(*trim_exprs)
    
    # Change data type logic.
    def change_data_type(self, df: DataFrame) -> DataFrame:
        """
        Change data type of columns based on schema_detail.
        For date columns, uses try_to_date() with "MMMM d, yyyy" format.
        For other columns, uses try_cast() to return NULL for invalid conversions.
        Records with NULL values can be caught later in get_invalid_record().
        """
        # Pre-compute columns to avoid repeated Analyze RPCs
        df_columns = df.columns
        
        change_type_exprs = []
        
        for col_name, col_type in self.schema_detail.items():
            # Generic date handling - assumes "MMMM d, yyyy" format (handles both single and double digit days)
            if col_type == "date":
                change_type_exprs.append(
                    expr(f"try_to_date({col_name}, 'MMMM d, yyyy')").alias(col_name)
                )
            else:
                change_type_exprs.append(
                    expr(f"try_cast({col_name} as {col_type})").alias(col_name)
                )
        
        # Include _sk if it exists (using pre-computed df_columns)
        if "_sk" in df_columns:
            change_type_exprs.append(col("_sk"))
        
        return df.select(*change_type_exprs)
    
    # Get invalid record logic.
    def get_invalid_record(self, bronze_df: DataFrame) -> DataFrame:
        '''
        Separate invalid record based on failed type conversions (NULL values).
        Called AFTER change_data_type(), so invalid conversions appear as NULL.
        try_cast() returns NULL for any conversion failure (format errors, overflow, etc.).
        '''
        invalid_col = {
            f"_is_{col_name}_invalid": col(col_name).isNull()
            for col_name, col_type in self.schema_detail.items() if col_type in ["int", "date"]
        }
        
        return (
            bronze_df
            .withColumns(invalid_col)
            .transform(self._get_reason)
        )
    
    # Get key null record logic.
    def get_key_null_record(self, bronze_df: DataFrame) -> DataFrame:
        '''
        Separate key null record prepare union with invalid record and duplicate record to write into bad record table
        '''
        key_null_statement = { f"_is_{col_name}_null": col(col_name).isNull() for col_name in self.keys}

        return (
            bronze_df.withColumns(key_null_statement)
            .transform(self._get_reason)
        )
    
    # Get invalid show_id pattern logic.
    def get_invalid_show_id_record(self, bronze_df: DataFrame) -> DataFrame:
        '''
        Validate show_id follows the expected pattern: 's' + digits (e.g., s1, s74, s8809).
        Records with invalid patterns (e.g., "Flying Fortress", " and probably will.") are flagged.
        This catches CSV corruption where non-show_id values end up in the show_id column.
        '''
        return (
            bronze_df
            .withColumn("_is_show_id_invalid", ~col("show_id").rlike("^s\\d+$"))
            .transform(self._get_reason)
        )
    
    # Get duplicate record logic.
    def get_dup_record(self, bronze_df: DataFrame, key_null_df: DataFrame) -> DataFrame:
        '''
        Separate duplicate record prepare union with invalid record and duplicate record to write into bad record table
        '''
        partition_by_all = Window.partitionBy(*self.data_col).orderBy("_sk")
        partition_by_key = Window.partitionBy(*self.keys)

        bronze_not_null_df = bronze_df.join(key_null_df, ['_sk'], "left_anti")

        is_row_duplicate_df = (
            bronze_not_null_df
            .withColumn("rn", row_number().over(partition_by_all))
            .filter(col("rn") > 1)
            .drop("rn")
            .withColumn("reason", array(lit("_row_duplication")))
        )

        is_key_duplication_df = (
            bronze_not_null_df
            .join(is_row_duplicate_df, ['_sk'], "left_anti")
            .withColumn("key_count", count("*").over(partition_by_key))
            .filter(col("key_count") > 1)
            .drop("key_count")
            .withColumn("reason", array(lit("_key_duplicate")))
        )
        return (
            is_row_duplicate_df
            .unionByName(is_key_duplication_df)
        )

    # Get all bad record logic.
    def get_all_bad_record(self, invalid_df: DataFrame, key_null_df: DataFrame, invalid_show_id_df: DataFrame, duplicate_df: DataFrame) -> DataFrame:
        '''
        Union all bad record prepare for write into bad record table.
        Includes: invalid type conversions, null keys, invalid show_id patterns, and duplicates.
        '''
        return (
            invalid_df
            .unionByName(key_null_df)
            .unionByName(invalid_show_id_df)
            .unionByName(duplicate_df)
            .groupBy(*self.data_col, "_sk")
            .agg(flatten(collect_list("reason")).alias("reason"))
        )

    # Get final result logic.
    def get_final_result(self, bronze_df: DataFrame, all_bad_df: DataFrame) -> DataFrame:
        '''
        Get only good record by dropping bad record.
        '''
        add_control_col = {"load_dt" : current_date()
                           , "load_dttm": current_timestamp()}
        return (
            bronze_df
            .join(all_bad_df, ["_sk"], "left_anti")
            .select(*self.data_col, "_sk")
            .withColumns(add_control_col)
        )

    # Get hash key and value logic.
    def get_hash_key_value(self, final_df: DataFrame) -> DataFrame:
        '''
        Add hash key and hash value to final result for more efficient join later.
        '''
        # These column use to explode so we don't use them in the hash key
        columns_to_explode = ["cast", "director", "country", "listed_in"]
        columns_to_hash = [col_name for col_name in self.data_col 
                           if col_name not in self.keys and col_name not in columns_to_explode]
        # Create hash key and hash value
        df_with_hash = (
            final_df
            .withColumn("hash_key", sha2(concat_ws("||", *[col(key) for key in self.keys]), 256))
            .withColumn("hash_value", sha2(concat_ws("||", *[col(value) for value in columns_to_hash]), 256))
        )
        columns_to_drop = columns_to_explode + ["_sk"]
        # Drop columns we don't need
        final_main_dimension_df = df_with_hash.drop(*columns_to_drop)
        return final_main_dimension_df

    # Load bad record logic.
    def load_bad_record(self, all_bad_df: DataFrame, batch_id: int) -> None:
        """
        Load bad records to the bad record table with metadata.
        Appends records with batch_id, load_dt, and load_dttm for tracking.
        """
        if all_bad_df.isEmpty():
            print(f"Batch {batch_id}: No bad records to load")
            return
        
        # Add metadata columns for tracking
        bad_records_with_metadata = (
            all_bad_df
            .withColumn("batch_id", lit(batch_id))
            .withColumn("load_dt", current_date())
            .withColumn("load_dttm", current_timestamp())
        )
        
        # Write to bad record table
        bad_records_with_metadata.write.mode("append").saveAsTable(self.bad_record_table_name)
        
        print(f"Batch {batch_id}: Loaded {all_bad_df.count()} bad records to {self.bad_record_table_name}")

    # ==========================================================================
    # HELPER METHODS FOR EXPLODING DIMENSIONS & BRIDGES
    # ==========================================================================
    
    def _transform_and_explode_dimension(self, final_df: DataFrame, source_col: str, target_col_name: str, id_col_name: str) -> DataFrame:
        """
        Main method for exploding a dimension column for create sub dimension tables (Distinct)
        """
        return (
            final_df
            .select(source_col)
            .filter(col(source_col).isNotNull())
            .withColumn(target_col_name, explode(split(col(source_col), ",")))
            .withColumn(target_col_name, initcap(trim(col(target_col_name))))
            .filter(col(target_col_name) != "")
            .select(target_col_name).distinct()
            .withColumn(id_col_name, sha2(col(target_col_name), 256))
            .select(id_col_name, target_col_name)
        )

    def _transform_and_explode_bridge(self, final_df: DataFrame, source_col: str, target_col_name: str, id_col_name: str) -> DataFrame:
        """
        Central method for exploding a bridge column for create bridge tables (Many-to-Many) with _sk
        """
        return (
            final_df
            .select("show_id", "_sk", source_col)
            .filter(col(source_col).isNotNull())
            .withColumn(target_col_name, explode(split(col(source_col), ",")))
            .withColumn(target_col_name, initcap(trim(col(target_col_name))))
            .filter(col(target_col_name) != "")
            .withColumn(id_col_name, sha2(col(target_col_name), 256))
            .select("show_id", "_sk", id_col_name)
        )

    def load_sub_dimensions(self, final_df: DataFrame, batch_id: int = None) -> None:
        """
        Load data into 4 sub-dimension tables (cast, director, country, category).
        Each dimension table stores unique master data.
        """
        configs = [
            ("cast", "cast_name", "cast_id", "workspace.netflix.dim_cast_silver"),
            ("director", "director_name", "director_id", "workspace.netflix.dim_directors_silver"),
            ("country", "country_name", "country_id", "workspace.netflix.dim_countries_silver"),
            ("listed_in", "category_name", "category_id", "workspace.netflix.dim_categories_silver")
        ]

        batch_msg = f" (Batch {batch_id})" if batch_id is not None else ""
        print(f"\n--- Loading Sub-Dimension Tables{batch_msg} ---")
        
        for src_col, target_name, id_name, dim_table in configs:
            dim_df = self._transform_and_explode_dimension(final_df, src_col, target_name, id_name)
            target_dim = DeltaTable.forName(spark, dim_table)
            (target_dim.alias("target")
             .merge(dim_df.alias("source"), f"target.{id_name} = source.{id_name}")
             .whenNotMatchedInsertAll()
             .execute())
            print(f"-> {dim_table}: Loaded")
        
        print(f" All 4 sub-dimension tables loaded{batch_msg}")

    def load_bridge_tables(self, final_df: DataFrame, batch_id: int = None) -> None:
        """
        Load data into 4 bridge tables (title_cast, title_director, title_country, title_category).
        Bridge tables handle many-to-many relationships using _sk and dimension IDs.
        """
        configs = [
            ("cast", "cast_name", "cast_id", "workspace.netflix.bridge_title_cast_silver"),
            ("director", "director_name", "director_id", "workspace.netflix.bridge_title_director_silver"),
            ("country", "country_name", "country_id", "workspace.netflix.bridge_title_country_silver"),
            ("listed_in", "category_name", "category_id", "workspace.netflix.bridge_title_category_silver")
        ]

        batch_msg = f" (Batch {batch_id})" if batch_id is not None else ""
        print(f"\n--- Loading Bridge Tables{batch_msg} ---")
        
        for src_col, target_name, id_name, bridge_table in configs:
            bridge_df = self._transform_and_explode_bridge(final_df, src_col, target_name, id_name)
            target_bridge = DeltaTable.forName(spark, bridge_table)
            (target_bridge.alias("target")
             .merge(bridge_df.alias("source"), f"target._sk = source._sk AND target.{id_name} = source.{id_name}")
             .whenNotMatchedInsertAll()
             .execute())
            print(f"-> {bridge_table}: Loaded")
        
        print(f" All 4 bridge tables loaded{batch_msg}")


    # ==========================================================================
    # MAIN PIPELINE WORKFLOW (ENTRY POINT)
    # ==========================================================================

    def load_to_silver_layer(self, final_df: DataFrame, batch_id: int = None) -> None:
        """
        Load data into the main dimension table (dim_titles_silver) with SCD Type 2.
        This method focuses solely on the main fact/dimension table.
        Sub-dimensions and bridge tables are handled separately.
        
        Args:
            final_df: DataFrame with clean data (after quality checks)
            batch_id: The batch number for tracking/logging
        """
        # Apply hash key transformation (removes _sk and explodable columns)
        final_df_with_hash = self.get_hash_key_value(final_df)
        
        batch_msg = f" (Batch {batch_id})" if batch_id is not None else ""
        print(f"\n--- Loading Main Dimension Table{batch_msg} ---")
        
        # Get target main dimension table
        target_main_table = DeltaTable.forName(spark, "workspace.netflix.dim_titles_silver")
        
        # ------------------------------------------------------------------
        # STEP 1: SCD TYPE 2 - Close historical changed rows
        # ------------------------------------------------------------------
        print("-> [SCD Type 2] Step 1: Closing historical changed rows...")
        (target_main_table.alias("target")
         .merge(
             final_df_with_hash.alias("source"),
             "target.show_id = source.show_id AND target.active_flag = true"
         )
         .whenMatchedUpdate(
             condition = "target.hash_value <> source.hash_value",
             set = {
                 "active_flag": "false",
                 "end_date": "current_timestamp()"
             }
         )
         .execute())

        # ------------------------------------------------------------------
        # STEP 2: SCD TYPE 2 - Insert new/updated rows
        # ------------------------------------------------------------------
        print("-> [SCD Type 2] Step 2: Inserting latest current rows...")
        
        # Prepare insert statement with only columns that exist in target table
        insert_values = {
            "show_id": "source.show_id",
            "hash_key": "source.hash_key",
            "hash_value": "source.hash_value",
            "active_flag": "true",
            "start_date": "current_timestamp()",
            "end_date": "cast(null as timestamp)"
        }
        # Add data columns that exist in both source and target
        data_columns_in_target = ["type", "title", "release_year", "rating", "duration", "description"]
        for col_name in data_columns_in_target:
            if col_name in final_df_with_hash.columns:
                insert_values[col_name] = f"source.{col_name}"

        (target_main_table.alias("target")
         .merge(
             final_df_with_hash.alias("source"),
             "target.hash_key = source.hash_key AND target.active_flag = true"
         )
         .whenNotMatchedInsert(values = insert_values)
         .execute())
        
        print(f" Main dimension table loaded{batch_msg} (SCD Type 2 Complete!)")




    def process_cdf_stream_to_silver(self, checkpoint_location: str = None) -> None:
        """
        Process CDF stream from bronze to silver with quality checks.
        Uses trigger(availableNow=True) for serverless-friendly incremental processing.
        """
        # Use provided checkpoint location or default to table-specific path
        if checkpoint_location is None:
            checkpoint_location = f"/Volumes/workspace/netflix/checkpoint_dir/{self.table_name}_silver/"
            
        # Create a stream from the bronze table
        # Note: _sk is added in _process_quality_checks_batch to avoid collision across batches
        cdf_stream = (
            spark.readStream
            .option("readChangeFeed", "true")
            .option("startingVersion", 0)  # Start from version 0 or use checkpoint
            .table(self.bronze_table_name)
            .filter(col("_change_type").isin(["insert", "update_postimage"]))
            .select(*self.data_col)
        )

        query = (
            cdf_stream.writeStream
            .foreachBatch(self._process_quality_checks_batch)
            .option("checkpointLocation", checkpoint_location)
            .outputMode("append") # Only use when we use foreachBatch 
            .trigger(availableNow=True)
            .start()
        )
    
        query.awaitTermination()
        print(f"Stream processing complete for {self.silver_table_name}")

    def _process_quality_checks_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """
        Process each micro-batch through quality checks.
        Called automatically by foreachBatch with (batch_df, batch_id).
        """
        if batch_df.isEmpty():
            return
        
        # Add unique surrogate key: combine batch_id with monotonically_increasing_id()
        # This ensures _sk is unique across all batches
        batch_with_sk = batch_df.withColumn(
            "_sk",
            (lit(batch_id).cast("long") * 1000000000) + monotonically_increasing_id()
        )
        
        # Standardize and clean DataFrame
        trimmed_stream = self.trim_data(batch_with_sk)
        change_data_type_stream = self.change_data_type(trimmed_stream)
        invalid_df = self.get_invalid_record(change_data_type_stream)
        key_null_df = self.get_key_null_record(change_data_type_stream)
        invalid_show_id_df = self.get_invalid_show_id_record(change_data_type_stream)
        duplicate_df = self.get_dup_record(change_data_type_stream, key_null_df)
        all_bad_df = self.get_all_bad_record(invalid_df, key_null_df, invalid_show_id_df, duplicate_df)
        final_df = self.get_final_result(change_data_type_stream, all_bad_df)

        # Load DataFrame into 4 sub-dimension and 4 bridge tables
        # Note: Use original final_df (with _sk) for bridge tables
        self.load_sub_dimensions(final_df, batch_id)
        self.load_bridge_tables(final_df, batch_id)
        
        # Load DataFrame into main dimension table and bad record table
        self.load_bad_record(all_bad_df, batch_id)
        self.load_to_silver_layer(final_df, batch_id)
        
        batch_msg = f" (Batch {batch_id})" if batch_id is not None else ""
        print("==================================================")
        print(f"BATCH PROCESSING COMPLETE{batch_msg}")
        print("==================================================")


# Gold Layer Class definition
@dataclass
class GoldLayer():
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

    # Define how to retrive variable from config table    
    @classmethod
    def from_config_table(cls, pipeline_name: str) -> "GoldLayer":
        conf = (
            spark.table("workspace.netflix.config_table")
            .filter(col("pipeline_name") == pipeline_name)
            .select(
                "table_name", "keys", "write_mode"
            )
            .first()
        )
        return cls(
            table_name = conf.table_name,
            keys = conf.keys,
            write_mode = conf.write_mode
        )

    def create_gold_content_by_cast(self) -> None:
        '''
        Insight: Which cast perform in what's title
        Logic: 
            Only retrive current record(Active) from dim_titles_silver
            Join with bridge_title_cast_silver table and dim_cast_silver
        '''
        # Retrive only current version of dim_titles_silver
        title_active_df = (
            spark.table("workspace.netflix.dim_titles_silver")
            .filter(col("active_flag") == True)
        )
        
        # Retrive both bridge_title_cast_silver and dim_cast_silver 
        bridge_cast_df = spark.table("workspace.netflix.bridge_title_cast_silver")
        cast_df = spark.table("workspace.netflix.dim_cast_silver")

        # Join all three tables
        flattened_cast_df = (
            title_active_df.alias("t")
            .join(bridge_cast_df.alias("b"), col("t.show_id") == col("b.show_id"), "inner")
            .join(cast_df.alias("c"), col("b.cast_id") == col("c.cast_id"), "inner")
            .drop(col("b.cast_id")) # Drop duplicate cast_id from bridge table
            .drop(col("t.show_id")) # Drop duplicate show_id from bridge table
        )

        # Write to gold table name "gold_table_content_by_cast"
        flattened_cast_df.write.format("delta").mode(f"{self.write_mode}").saveAsTable(f"{self.gold_table_content_by_cast}")
    
    def create_gold_yearly_content_trends(self) -> None:
        '''
        Insight: For each year there are how content uploaded increase or decrease by type
        Logic:
            GROUP BY release_year, type and count number of content
        '''
        # Retrive only current version of dim_titles_silver
        title_df = (
            spark.table("workspace.netflix.dim_titles_silver")
            .filter(col("active_flag") == True)
            )
        # Group by release_year and type
        summary_trends_df = (
            title_df
            .groupBy(col("release_year"), col("type"))
            .agg(count("show_id").alias("total_title"))
            .orderBy(col("release_year").desc(), col("type"))
        )
        
        # Write to gold table name "gold_yearly_content_trends"
        summary_trends_df.write.format("delta").mode(f"{self.write_mode}").saveAsTable(f"{self.gold_yearly_content_trends}")

    def run_gold_pipeline(self) -> None:
        '''
        Run all gold pipeline
        '''
        self.create_gold_content_by_cast()
        self.create_gold_yearly_content_trends()

