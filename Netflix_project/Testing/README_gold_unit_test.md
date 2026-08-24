# Gold Layer Unit Testing

## Overview
Unit tests for Netflix Gold Layer using `unittest.TestCase` with mocked Spark tables.

## Test File
* **gold_unit_test.py** - Complete test suite with 9 test cases

## Running Tests

### From Command Line
```bash
cd /Workspace/Users/pongsakronk009@hotmail.com/Databricks-for-Data-Engineers-Bootcamp2/Netflix_project/Testing
python gold_unit_test.py
```

### From Notebook
```python
import subprocess
import sys

result = subprocess.run(
    [sys.executable, "-m", "unittest", "gold_unit_test", "-v"],
    cwd="/Workspace/Users/pongsakronk009@hotmail.com/Databricks-for-Data-Engineers-Bootcamp2/Netflix_project/Testing",
    capture_output=True,
    text=True
)

print(result.stdout)
if result.stderr:
    print(result.stderr)
```

### Run Specific Test
```bash
python -m unittest gold_unit_test.TestGoldLayerWithMocks.test_empty_titles_table
```

## Test Cases

### 1. test_initialization
Validates GoldLayer class attributes are set correctly.

### 2. test_empty_titles_table ⭐
Tests behavior with completely empty input tables.

### 3. test_no_active_records ⭐
Tests when all records have `active_flag=False`.

### 4. test_missing_cast_in_bridge ⭐
Tests invalid cast references in bridge table.

### 5. test_yearly_trends_with_empty_data ⭐
Tests aggregation with zero rows.

### 6. test_yearly_trends_aggregation
Validates correct GROUP BY counts.

### 7. test_null_values_in_data ⭐
Tests NULL handling in title and release_year.

### 8. test_duplicate_show_ids ⭐
Tests SCD Type 2 with multiple versions per show_id.

### 9. test_write_mode_overwrite
Validates overwrite mode replaces data.

⭐ = Edge case test

## Key Features

✅ **Uses unittest.TestCase** - Standard Python testing framework  
✅ **Mocked Tables** - No real data touched  
✅ **Edge Cases** - Empty, NULL, invalid, duplicate scenarios  
✅ **Isolated Tests** - Each test is independent  
✅ **Fast** - Runs in ~15 seconds  

## Mock Data Architecture

Tests create temporary Spark DataFrames and register them as views:

```python
# Create mock data
titles_df = self._create_mock_titles_df(data=[...])

# Register as temp view (simulates real table)
titles_df.createOrReplaceTempView("workspace.netflix.dim_titles_silver")

# GoldLayer reads from mock instead of real table
gold.create_gold_content_by_cast()
```

## Expected Output

```
test_duplicate_show_ids ... ok
test_empty_titles_table ... ok
test_initialization ... ok
test_missing_cast_in_bridge ... ok
test_no_active_records ... ok
test_null_values_in_data ... ok
test_write_mode_overwrite ... ok
test_yearly_trends_aggregation ... ok
test_yearly_trends_with_empty_data ... ok

----------------------------------------------------------------------
Ran 9 tests in 15.234s

OK
```

## Troubleshooting

### "Table or view not found"
Ensure you call `createOrReplaceTempView()` before running gold methods.

### "ModuleNotFoundError: No module named 'pyspark'"
```bash
%pip install pyspark
```

### "Spark session already exists"
Use `tearDownClass` to stop the session:
```python
@classmethod
def tearDownClass(cls):
    cls.spark.stop()
```
