# Advanced Pipeline Examples

This document contains advanced examples and edge cases for bauplan pipelines.

> **Default to Python models.** Use SQL models only as first nodes reading directly from lakehouse tables, and only when the data volume justifies it.

## Contents

- [Output Columns Validation Example](#output-columns-validation-example)
- [Materialization Strategies](#materialization-strategies) (REPLACE, APPEND)
- [DuckDB in Python Models](#duckdb-in-python-models)
- [Multi-Input Model](#multi-input-model)
- [I/O Pushdown with Column Selection and Filtering](#io-pushdown-with-column-selection-and-filtering)
- [Data Quality Expectations](#data-quality-expectations)
- [Multi-Stage Pipeline Example](#multi-stage-pipeline-example)
- [Available Built-in Expectations](#available-built-in-expectations)

## Output Columns Validation Example

This example demonstrates the `columns` parameter in `@bauplan.model()` for output schema validation.

**Scenario**: Source table `titanic` has the following schema:

| passenger_id | name    | age | sex    | embarked |
|--------------|---------|-----|--------|----------|
| 1            | Alice   | 30  | female | S        |
| 2            | Bob     | 25  | male   | C        |

A model that drops the `embarked` column should declare its output columns in the following way:

```python
import bauplan

# these columns are the expected output - in fact `embarked` is not there
@bauplan.model(columns=['passenger_id', 'name', 'age', 'sex'])
@bauplan.python('3.11')
def titanic_clean(
    data=bauplan.Model(
        'titanic',
        # these columns are the input to the model - in fact, `embarked` is present
        columns=['passenger_id', 'name', 'age', 'sex', 'embarked']
    )
):
    """
    Removes the embarked column from titanic data.

    | passenger_id | name  | age | sex    |
    |--------------|-------|-----|--------|
    | 1            | Alice | 30  | female |
    | 2            | Bob   | 25  | male   |
    """
    return data.drop_columns(['embarked'])
```

**Key points:**
1. First, check the source table schema with `bauplan table get bauplan.titanic`
2. Determine which columns your transformation produces
3. Specify `columns=[...]` in `@bauplan.model()` to enable output validation
4. Reason on how the tables change as they flow through the pipeline, so that you can accurately declare output schemas for all the downstream models as well

## Materialization Strategies
The `materialization_strategy` parameter controls how model output is persisted.

### NONE (default) - In-memory only
Streams output as Arrow table without persisting to storage:

```python
@bauplan.model()  # No materialization_strategy = NONE
@bauplan.python('3.11')
def intermediate_transform(data=bauplan.Model('source')):
    """Intermediate step, not persisted to lakehouse."""
    return data.filter(...)
```

### REPLACE - Full table overwrite
Replaces entire table on each run. Use for most pipelines:

```python
@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11')
def daily_summary(data=bauplan.Model('events')):
    """Overwrites previous results completely."""
    return data
```

### APPEND - Incremental loads
Adds new rows to existing table:

```python
@bauplan.model(materialization_strategy='APPEND')
@bauplan.python('3.11')
def event_log(data=bauplan.Model('new_events', filter="date = CURRENT_DATE")):
    """Appends today's events to historical table."""
    return data
```
### SQL Syntax
For SQL models,define the materialization strategy using a comment:

```sql
-- product_catalog.sql
-- bauplan: materialization_strategy=REPLACE

SELECT product_id, name, price FROM raw_products
```

### OVERWRITE_PARTITIONS - Selective partition replacement
Replaces rows matching `overwrite_filter` while preserving others. Requires `partitioned_by`:

```python
@bauplan.model(
    partitioned_by=['day(pickup_datetime)'],
    materialization_strategy='OVERWRITE_PARTITIONS',
    overwrite_filter="pickup_datetime >= '2024-01-15' AND pickup_datetime < '2024-01-16'"
)
@bauplan.python('3.11')
def partitioned_trips(data=bauplan.Model('trips', filter="pickup_datetime >= '2024-01-15'")):
    """Replaces only the specified day partition."""
    return data
```

## DuckDB in Python Models
Use DuckDB for SQL-like transformations in Python. DuckDB can be imported like any other Python dependency using the `@bauplan.python()` decorator:
Always use `columns` and `filter` for I/O pushdown in the model input:

```python
@bauplan.model(
    materialization_strategy='REPLACE',
    columns=['purchase_session', 'event_hour', 'session_count', 'total_revenue', 'avg_order_value']
)
@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
def purchase_analytics(
    # Use columns and filter for I/O pushdown
    events=bauplan.Model(
        'ecommerce_events',
        columns=['user_session', 'event_time', 'event_type', 'price'],
        filter="event_type = 'purchase'"
    )
):
    """
    Aggregates purchase events by session and hour.

    | purchase_session | event_hour          | session_count | total_revenue | avg_order_value |
    |------------------|---------------------|---------------|---------------|-----------------|
    | abc123           | 2024-01-01 10:00:00 | 3             | 150.00        | 50.00           |
    """
    import duckdb

    con = duckdb.connect()
    con.register("events", events)

    query = """
        SELECT
            user_session AS purchase_session,
            DATE_TRUNC('hour', event_time) AS event_hour,
            COUNT(*) AS session_count,
            SUM(price) AS total_revenue,
            AVG(price) AS avg_order_value
        FROM events
        GROUP BY 1, 2
        ORDER BY 2 ASC
    """
    return con.execute(query).fetch_arrow_table()
```

### Python Model with Multiple Inputs
Models can take multiple tables as input - just add more `bauplan.Model()` parameters:
```python
@bauplan.model(
    materialization_strategy='REPLACE',
    columns=['user_session', 'pickup_datetime', 'trip_miles', 'Borough', 'Zone']
)
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def trips_with_zones(
    trips=bauplan.Model(
        'taxi_trips',
        columns=['user_session', 'pickup_datetime', 'trip_miles', 'PULocationID']
    ),
    zones=bauplan.Model(
        'taxi_zones',
        columns=['LocationID', 'Borough', 'Zone']
    )
):
    """
    Joins trips with zone information.

    | user_session | pickup_datetime     | trip_miles | Borough   | Zone    |
    |--------------|---------------------|------------|-----------|---------|
    | abc123       | 2024-01-01 10:00:00 | 5.2        | Manhattan | Midtown |
    """
    import polars as pl

    trips_df = pl.from_arrow(trips)
    zones_df = pl.from_arrow(zones)

    result = trips_df.join(
        zones_df,
        left_on='PULocationID',
        right_on='LocationID'
    ).drop('PULocationID', 'LocationID')

    return result.to_arrow()
```
## I/O Pushdown with Column Selection and Filtering

> **CRITICAL**: Always use `columns` and `filter` parameters to enable I/O pushdown. This restricts data at the storage level, dramatically reducing data transfer and improving performance.

```python
@bauplan.model(columns=[
    'pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID',
    'trip_miles', 'base_passenger_fare', 'Borough', 'Zone'
])
@bauplan.python('3.11')
def optimized_model(
    trips=bauplan.Model(
        'taxi_fhvhv',
        columns=[
            'pickup_datetime',
            'dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'trip_miles',
            'base_passenger_fare'
        ],
        filter="pickup_datetime >= '2022-12-01' AND pickup_datetime < '2023-01-01'"
    ),
    zones=bauplan.Model(
        'taxi_zones',
        columns=['LocationID', 'Borough', 'Zone']
    ),
):
    """
    Joins trips with zone data for December 2022.

    | pickup_datetime     | ... | PULocationID | trip_miles | Borough   | Zone    |
    |---------------------|-----|--------------|------------|-----------|---------|
    | 2022-12-01 08:00:00 | ... | 123          | 5.2        | Manhattan | Midtown |
    """
    result = trips.join(zones, 'PULocationID', 'LocationID')
    return result.combine_chunks()
```

## Data Quality Expectations

Create `expectations.py` in your project folder:

```python
import bauplan
from bauplan.standard_expectations import (
    expect_column_no_nulls,
    expect_column_all_unique,
    expect_column_mean_smaller_than
)

@bauplan.expectation()
@bauplan.python('3.11')
def test_no_null_ids(data=bauplan.Model('clean_orders')):
    result = expect_column_no_nulls(data, 'order_id')
    assert result, 'order_id must not contain null values'
    return result

@bauplan.expectation()
@bauplan.python('3.11')
def test_unique_order_ids(data=bauplan.Model('clean_orders')):
    result = expect_column_all_unique(data, 'order_id')
    assert result, 'order_id must be unique'
    return result

@bauplan.expectation()
@bauplan.python('3.11')
def test_reasonable_trip_distance(data=bauplan.Model('clean_trips')):
    # Average trip should be < 50 miles
    upper_bound = expect_column_mean_smaller_than(data, 'trip_miles', 50.0)
    assert upper_bound, 'Average trip distance out of expected range'
    return upper_bound
```

## Multi-Stage Pipeline Example

This example demonstrates a complete e-commerce analytics pipeline with:
- Python models for all transformations (cleaning, aggregation, summarization)
- I/O pushdown for efficient data reading
- Data quality expectations

### Pipeline DAG
```text
[lakehouse: raw_ecommerce_events]
           │
           ▼
      [staging]            ← Python model (cleans raw data)
           │
           ▼
   [session_metrics]       ← Python model (aggregates to sessions)
           │
           ▼
   [daily_summary]         ← Python model (final output)
```

All three outputs are materialized (`REPLACE` strategy).

### Project Structure
```text
ecommerce-pipeline/
  bauplan_project.yml
  models.py             # All pipeline models: staging, session_metrics, daily_summary
  expectations.py       # Data quality checks
```

### bauplan_project.yml
```yaml
project:
  id: 550e8400-e29b-41d4-a716-446655440000
  name: ecommerce_analytics
```

### models.py

Key patterns used:
- `columns` and `filter` parameters for I/O pushdown
- `materialization_strategy='REPLACE'` for persisted outputs
- Docstrings with output schema as ASCII tables

```python
import bauplan


# ============================================================================
# Stage 1: Clean raw data
# ============================================================================

@bauplan.model(
    materialization_strategy='REPLACE',
    columns=['event_id', 'event_type', 'product_id', 'brand',
             'price', 'user_id', 'user_session', 'event_time']
)
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def staging(
    raw=bauplan.Model(
        'raw_ecommerce_events',
        columns=['event_id', 'event_type', 'product_id', 'brand',
                 'price', 'user_id', 'user_session', 'event_time'],
        filter="event_time IS NOT NULL AND price > 0"
    )
):
    """
    Cleans raw e-commerce events: normalizes event_type, fills missing brands,
    and casts price to decimal.

    | event_id | event_type | product_id | brand   | price  | user_id | user_session | event_time          |
    |----------|------------|------------|---------|--------|---------|--------------|---------------------|
    | evt_001  | view       | prod_123   | Nike    | 99.99  | usr_001 | sess_abc     | 2024-01-01 10:00:00 |
    | evt_002  | purchase   | prod_456   | Unknown | 149.50 | usr_002 | sess_def     | 2024-01-01 10:05:00 |
    """
    import polars as pl

    df = pl.from_arrow(raw)

    result = df.with_columns([
        pl.col('event_type').str.to_lowercase(),
        pl.col('brand').fill_null('Unknown'),
        pl.col('price').cast(pl.Decimal(10, 2)),
        pl.col('event_time').cast(pl.Datetime)
    ])

    return result.to_arrow()


# ============================================================================
# Stage 2: Aggregate to session-level metrics
# ============================================================================

@bauplan.model(
    materialization_strategy='REPLACE',
    columns=['user_session', 'session_start', 'session_end', 'total_events',
             'products_viewed', 'purchases', 'session_revenue']
)
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def session_metrics(
    staging=bauplan.Model(
        'staging',
        columns=['user_session', 'event_time', 'product_id', 'event_type', 'price']
    )
):
    """
    Aggregates events into session-level metrics.

    | user_session | session_start       | session_end         | total_events | products_viewed | purchases | session_revenue |
    |--------------|---------------------|---------------------|--------------|-----------------|-----------|-----------------|
    | sess_abc     | 2024-01-01 10:00:00 | 2024-01-01 10:30:00 | 15           | 5               | 2         | 150.00          |
    """
    import polars as pl

    df = pl.from_arrow(staging)

    result = df.group_by('user_session').agg([
        pl.col('event_time').min().alias('session_start'),
        pl.col('event_time').max().alias('session_end'),
        pl.len().alias('total_events'),
        pl.col('product_id').n_unique().alias('products_viewed'),
        (pl.col('event_type') == 'purchase').sum().alias('purchases'),
        pl.when(pl.col('event_type') == 'purchase')
          .then(pl.col('price'))
          .otherwise(0)
          .sum()
          .alias('session_revenue')
    ])

    return result.to_arrow()


# ============================================================================
# Stage 3: Daily summary (final output)
# ============================================================================

@bauplan.model(
    materialization_strategy='REPLACE',
    columns=['date', 'total_sessions', 'total_purchases', 'conversion_rate',
             'total_revenue', 'avg_session_revenue']
)
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def daily_summary(
    sessions=bauplan.Model(
        'session_metrics',
        columns=['session_start', 'purchases', 'session_revenue'],
        filter="purchases > 0"
    )
):
    """
    Computes daily summary metrics from sessions with purchases.

    | date       | total_sessions | total_purchases | conversion_rate | total_revenue | avg_session_revenue |
    |------------|----------------|-----------------|-----------------|---------------|---------------------|
    | 2024-01-01 | 500            | 150             | 30.00           | 15000.00      | 100.00              |
    """
    import polars as pl

    df = pl.from_arrow(sessions)

    result = df.group_by(
        pl.col('session_start').dt.truncate('1d').alias('date')
    ).agg([
        pl.len().alias('total_sessions'),
        pl.col('purchases').sum().alias('total_purchases'),
        (pl.col('purchases').sum() / pl.len() * 100).round(2).alias('conversion_rate'),
        pl.col('session_revenue').sum().alias('total_revenue'),
        pl.col('session_revenue').mean().round(2).alias('avg_session_revenue')
    ]).sort('date')

    return result.to_arrow()
```

### expectations.py

Expectations validate data quality after the pipeline runs. They must return `True` (pass) or raise an exception (fail).
```python
import bauplan
from bauplan.standard_expectations import expect_column_no_nulls


@bauplan.expectation()
@bauplan.python('3.11')
def test_staging_completeness(data=bauplan.Model('staging')):
    """Verify critical columns have no null values."""
    for col in ['event_id', 'user_session', 'event_time']:
        result = expect_column_no_nulls(data, col)
        assert result, f'{col} contains null values'
    return True
```

### Running the Pipeline
```bash
# 1. Verify source table exists
bauplan table get bauplan.raw_ecommerce_events

# 2. create a branch to run
bauplan branch create <username>.<branch_name>
bauplan checkout <username>.<branch_name>

# 3. Dry run to validate DAG and output schemas
bauplan run --dry-run

# 4. Execute pipeline
bauplan run

# 5. Verify outputs
bauplan table get bauplan.daily_summary
bauplan query "SELECT * FROM bauplan.daily_summary LIMIT 5"
```

## Available Built-in Expectations

> **Note**: The table below shows example expectations from `bauplan.standard_expectations`. For the latest and complete list, consult the official SDK documentation: https://docs.bauplanlabs.com/reference/bauplan_standard_expectations

| Function                                   | Description                           |
|--------------------------------------------|---------------------------------------|
| `expect_column_no_nulls`                   | Column has no null values             |
| `expect_column_all_null`                   | Column is entirely null               |
| `expect_column_some_null`                  | Column has at least one null          |
| `expect_column_all_unique`                 | All values in column are unique       |
| `expect_column_not_unique`                 | Column has duplicate values           |
| `expect_column_accepted_values`            | Values are within allowed set         |
| `expect_column_mean_greater_than`          | Mean exceeds threshold                |
| `expect_column_mean_smaller_than`          | Mean below threshold                  |
| `expect_column_mean_greater_or_equal_than` | Mean >= threshold                     |
| `expect_column_mean_smaller_or_equal_than` | Mean <= threshold                     |
| `expect_column_equal_concatenation`        | Column equals concatenation of others |
