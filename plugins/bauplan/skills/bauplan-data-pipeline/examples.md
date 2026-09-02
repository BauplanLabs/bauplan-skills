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

This example declares output columns and their types with a `TableSchema` return annotation.

**Scenario**: Source table `titanic` has the following schema:

| passenger_id | name    | age | sex    | embarked |
|--------------|---------|-----|--------|----------|
| 1            | Alice   | 30  | female | S        |
| 2            | Bob     | 25  | male   | C        |

A model that drops the `embarked` column should declare its output columns in the following way:

```python
from typing import Annotated

import bauplan
import pyarrow as pa


class PassengerColumns(bauplan.TableSchema):
    """Passenger fields read before dropping embarkation."""

    passenger_id: bauplan.Int64 | None
    name: bauplan.String | None
    age: bauplan.Int64 | None
    sex: bauplan.String | None
    embarked: bauplan.String | None


class CleanPassengerSchema(bauplan.TableSchema):
    """Passenger output without embarkation."""

    passenger_id: bauplan.Int64 | None
    name: bauplan.String | None
    age: bauplan.Int64 | None
    sex: bauplan.String | None


@bauplan.model()
@bauplan.python('3.11')
def titanic_clean(
    data: Annotated[
        pa.Table, bauplan.Model('titanic', projection_schema=PassengerColumns)
    ],
) -> Annotated[pa.Table, CleanPassengerSchema]:
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
3. Declare `-> Annotated[pa.Table, OutputSchema]` for output validation
4. Reason on how the tables change as they flow through the pipeline, so that you can accurately declare output schemas for all the downstream models as well

## Materialization Strategies
The `materialization_strategy` parameter controls how model output is persisted.

The first three Python examples use this minimal event schema for both projection and output because they preserve the selected fields:

```python
from typing import Annotated

import bauplan
import pyarrow as pa


class EventSchema(bauplan.TableSchema):
    """Event identifiers and dates preserved by materialization examples."""

    event_id: bauplan.String | None
    date: bauplan.Date32 | None
```

### NONE (default) - In-memory only
Streams output as Arrow table without persisting to storage:

```python
@bauplan.model()  # No materialization_strategy = NONE
@bauplan.python('3.11')
def intermediate_transform(
    data: Annotated[pa.Table, bauplan.Model('source', projection_schema=EventSchema)],
) -> Annotated[pa.Table, EventSchema]:
    """Intermediate step, not persisted to lakehouse."""
    return data
```

### REPLACE - Full table overwrite
Replaces entire table on each run. Use for most pipelines:

```python
@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11')
def daily_summary(
    data: Annotated[pa.Table, bauplan.Model('events', projection_schema=EventSchema)],
) -> Annotated[pa.Table, EventSchema]:
    """Overwrites previous results completely."""
    return data
```

### APPEND - Incremental loads
Adds new rows to existing table:

```python
@bauplan.model(materialization_strategy='APPEND')
@bauplan.python('3.11')
def event_log(
    data: Annotated[
        pa.Table,
        bauplan.Model('new_events', projection_schema=EventSchema, filter="date = CURRENT_DATE"),
    ],
) -> Annotated[pa.Table, EventSchema]:
    """Appends today's events to historical table."""
    return data
```
### SQL Syntax
For SQL models,define the materialization strategy using a comment:

```sql
-- product_catalog.sql
-- bauplan: materialization_strategy=REPLACE
-- bauplan: output_schema = ProductCatalogSchema

SELECT product_id, name, price FROM raw_products
```

Define the SQL output schema in the sibling `models.py`:

```python
class ProductCatalogSchema(bauplan.TableSchema):
    """Product fields returned by the SQL catalog model."""

    product_id: bauplan.String | None
    name: bauplan.String | None
    price: bauplan.Float64 | None
```

### OVERWRITE_PARTITIONS - Selective partition replacement
Replaces rows matching `overwrite_filter` while preserving others. Requires `partitioned_by`:

```python
class PartitionedTripSchema(bauplan.TableSchema):
    """Trip timestamp and mileage for partition replacement."""

    pickup_datetime: bauplan.TimestampMicro | None
    trip_miles: bauplan.Float64 | None


@bauplan.model(
    partitioned_by=['day(pickup_datetime)'],
    materialization_strategy='OVERWRITE_PARTITIONS',
    overwrite_filter="pickup_datetime >= '2024-01-15' AND pickup_datetime < '2024-01-16'"
)
@bauplan.python('3.11')
def partitioned_trips(
    data: Annotated[
        pa.Table,
        bauplan.Model('trips', projection_schema=PartitionedTripSchema, filter="pickup_datetime >= '2024-01-15'"),
    ],
) -> Annotated[pa.Table, PartitionedTripSchema]:
    """Replaces only the specified day partition."""
    return data
```

## DuckDB in Python Models
Use DuckDB for SQL-like transformations in Python. DuckDB can be imported like any other Python dependency using the `@bauplan.python()` decorator:
Use `projection_schema` for needed columns and a literal `filter` for I/O pushdown:

```python
class PurchaseEventColumns(bauplan.TableSchema):
    """Event fields needed for purchase aggregation."""

    user_session: bauplan.String | None
    event_time: bauplan.TimestampMicro | None
    event_type: bauplan.String | None
    price: bauplan.Float64 | None


class PurchaseAnalyticsSchema(bauplan.TableSchema):
    """Purchase counts and revenue per session and hour."""

    purchase_session: bauplan.String | None
    event_hour: bauplan.TimestampMicro | None
    session_count: bauplan.Int64 | None
    total_revenue: bauplan.Float64 | None
    avg_order_value: bauplan.Float64 | None


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'duckdb': '1.0.0'})
def purchase_analytics(
    # Projection and filtering apply before the function receives the table (pushdown)
    events: Annotated[
        pa.Table,
        bauplan.Model('ecommerce_events', projection_schema=PurchaseEventColumns, filter="event_type = 'purchase'"),
    ],
) -> Annotated[pa.Table, PurchaseAnalyticsSchema]:
    """
    Aggregates purchase events by session and hour.

    | purchase_session | event_hour          | session_count | total_revenue | avg_order_value |
    |------------------|---------------------|---------------|---------------|-----------------|
    | abc123           | 2024-01-01 10:00:00 | 3             | 150.00        | 50.00           |
    """
    import duckdb  # ty: ignore[unresolved-import]

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
class SessionTripColumns(bauplan.TableSchema):
    """Trip fields needed to attach zone names."""

    user_session: bauplan.String | None
    pickup_datetime: bauplan.TimestampMicro | None
    trip_miles: bauplan.Float64 | None
    PULocationID: bauplan.Int64 | None


class ZoneColumns(bauplan.TableSchema):
    """Zone lookup key and labels."""

    LocationID: bauplan.Int64 | None
    Borough: bauplan.String | None
    Zone: bauplan.String | None


class TripsWithZonesSchema(bauplan.TableSchema):
    """Trips with zone labels and without join keys."""

    user_session: bauplan.String | None
    pickup_datetime: bauplan.TimestampMicro | None
    trip_miles: bauplan.Float64 | None
    Borough: bauplan.String | None
    Zone: bauplan.String | None


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def trips_with_zones(
    trips: Annotated[
        pa.Table, bauplan.Model('taxi_trips', projection_schema=SessionTripColumns)
    ],
    zones: Annotated[
        pa.Table, bauplan.Model('taxi_zones', projection_schema=ZoneColumns)
    ],
) -> Annotated[pa.Table, TripsWithZonesSchema]:
    """
    Joins trips with zone information.

    | user_session | pickup_datetime     | trip_miles | Borough   | Zone    |
    |--------------|---------------------|------------|-----------|---------|
    | abc123       | 2024-01-01 10:00:00 | 5.2        | Manhattan | Midtown |
    """
    import polars as pl  # ty: ignore[unresolved-import]

    trips_df = pl.DataFrame(trips)
    zones_df = pl.DataFrame(zones)

    result = trips_df.join(
        zones_df,
        left_on='PULocationID',
        right_on='LocationID'
    ).select('user_session', 'pickup_datetime', 'trip_miles', 'Borough', 'Zone')

    return result.to_arrow()
```
## I/O Pushdown with Column Selection and Filtering

> **CRITICAL**: Use `projection_schema` to select needed fields and a literal `filter` to restrict rows when applicable. This restricts data at the storage level, dramatically reducing data transfer and improving performance. `$param` templating inside the literal is supported; f-strings and filter variables are not. 

```python
class DecemberTripColumns(bauplan.TableSchema):
    """Trip fields needed for the December zone join."""

    pickup_datetime: bauplan.TimestampMicro | None
    dropoff_datetime: bauplan.TimestampMicro | None
    PULocationID: bauplan.Int64 | None
    DOLocationID: bauplan.Int64 | None
    trip_miles: bauplan.Float64 | None
    base_passenger_fare: bauplan.Float64 | None


class DecemberTripsSchema(bauplan.TableSchema):
    """December trips with zone labels."""

    pickup_datetime: bauplan.TimestampMicro | None
    dropoff_datetime: bauplan.TimestampMicro | None
    PULocationID: bauplan.Int64 | None
    DOLocationID: bauplan.Int64 | None
    trip_miles: bauplan.Float64 | None
    base_passenger_fare: bauplan.Float64 | None
    Borough: bauplan.String | None
    Zone: bauplan.String | None


# ZoneColumns is defined in the multiple-input example above
@bauplan.model()
@bauplan.python('3.11')
def optimized_model(
    trips: Annotated[
        pa.Table,
        bauplan.Model('taxi_fhvhv', projection_schema=DecemberTripColumns, filter="pickup_datetime >= '2022-12-01' AND pickup_datetime < '2023-01-01'"),
    ],
    zones: Annotated[
        pa.Table, bauplan.Model('taxi_zones', projection_schema=ZoneColumns)
    ],
) -> Annotated[pa.Table, DecemberTripsSchema]:
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
from typing import Annotated

import bauplan
import pyarrow as pa
from bauplan.standard_expectations import (
    expect_column_no_nulls,
    expect_column_all_unique,
    expect_column_mean_smaller_than
)

@bauplan.expectation()
@bauplan.python('3.11')
def test_no_null_ids(
    data: Annotated[pa.Table, bauplan.Model('clean_orders')],
) -> bool:
    """Order identifiers must be present."""
    result = expect_column_no_nulls(data, 'order_id')
    assert result, 'order_id must not contain null values'
    return result

@bauplan.expectation()
@bauplan.python('3.11')
def test_unique_order_ids(
    data: Annotated[pa.Table, bauplan.Model('clean_orders')],
) -> bool:
    """Order identifiers must be unique."""
    result = expect_column_all_unique(data, 'order_id')
    assert result, 'order_id must be unique'
    return result

@bauplan.expectation()
@bauplan.python('3.11')
def test_reasonable_trip_distance(
    data: Annotated[pa.Table, bauplan.Model('clean_trips')],
) -> bool:
    """Mean trip distance must remain below the expected bound."""
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
- `projection_schema` and literal `filter` parameters for I/O pushdown
- `materialization_strategy='REPLACE'` for persisted outputs
- Docstrings with output schema as ASCII tables

```python
from typing import Annotated, Any

import bauplan
import pyarrow as pa


class RawEventColumns(bauplan.TableSchema):
    """Raw event fields needed for staging."""

    event_id: bauplan.String | None
    event_type: bauplan.String | None
    product_id: bauplan.String | None
    brand: bauplan.String | None
    price: bauplan.Float64 | None
    user_id: bauplan.String | None
    user_session: bauplan.String | None
    event_time: bauplan.TimestampMicro | None


class StagingSchema(bauplan.TableSchema):
    """Normalized events with decimal prices."""

    event_id: bauplan.String | None
    event_type: bauplan.String | None
    product_id: bauplan.String | None
    brand: bauplan.String | None
    price: Any  # decimal column, no schema type available
    user_id: bauplan.String | None
    user_session: bauplan.String | None
    event_time: bauplan.TimestampMicro | None


class SessionEventColumns(bauplan.TableSchema):
    """Staged event fields needed for session aggregation."""

    user_session: bauplan.String | None
    event_time: bauplan.TimestampMicro | None
    product_id: bauplan.String | None
    event_type: bauplan.String | None
    price: Any  # decimal column, no schema type available


class SessionMetricsSchema(bauplan.TableSchema):
    """Session time windows, counts, and purchase revenue."""

    user_session: bauplan.String | None
    session_start: bauplan.TimestampMicro | None
    session_end: bauplan.TimestampMicro | None
    total_events: bauplan.Int64 | None
    products_viewed: bauplan.Int64 | None
    purchases: bauplan.Int64 | None
    session_revenue: Any  # decimal column, no schema type available


class DailySessionColumns(bauplan.TableSchema):
    """Session fields needed for the daily summary."""

    session_start: bauplan.TimestampMicro | None
    purchases: bauplan.Int64 | None
    session_revenue: Any  # decimal column, no schema type available


class DailySummarySchema(bauplan.TableSchema):
    """Daily counts, session conversion rate, and revenue across all sessions."""

    date: bauplan.TimestampMicro | None
    total_sessions: bauplan.Int64 | None
    total_purchases: bauplan.Int64 | None
    conversion_rate: bauplan.Float64 | None
    total_revenue: Any  # decimal column, no schema type available
    avg_session_revenue: bauplan.Float64 | None


# ============================================================================
# Stage 1: Clean raw data
# ============================================================================

@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def staging(
    raw: Annotated[
        pa.Table,
        bauplan.Model('raw_ecommerce_events', projection_schema=RawEventColumns, filter="event_time IS NOT NULL AND price > 0"),
    ],
) -> Annotated[pa.Table, StagingSchema]:
    """
    Cleans raw e-commerce events: normalizes event_type, fills missing brands,
    and casts price to decimal.

    | event_id | event_type | product_id | brand   | price  | user_id | user_session | event_time          |
    |----------|------------|------------|---------|--------|---------|--------------|---------------------|
    | evt_001  | view       | prod_123   | Nike    | 99.99  | usr_001 | sess_abc     | 2024-01-01 10:00:00 |
    | evt_002  | purchase   | prod_456   | Unknown | 149.50 | usr_002 | sess_def     | 2024-01-01 10:05:00 |
    """
    import polars as pl  # ty: ignore[unresolved-import]

    df = pl.DataFrame(raw)

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

@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def session_metrics(
    staging: Annotated[
        pa.Table, bauplan.Model('staging', projection_schema=SessionEventColumns)
    ],
) -> Annotated[pa.Table, SessionMetricsSchema]:
    """
    Aggregates events into session-level metrics.

    | user_session | session_start       | session_end         | total_events | products_viewed | purchases | session_revenue |
    |--------------|---------------------|---------------------|--------------|-----------------|-----------|-----------------|
    | sess_abc     | 2024-01-01 10:00:00 | 2024-01-01 10:30:00 | 15           | 5               | 2         | 150.00          |
    """
    import polars as pl  # ty: ignore[unresolved-import]

    df = pl.DataFrame(staging)

    result = df.group_by('user_session').agg([
        pl.col('event_time').min().alias('session_start'),
        pl.col('event_time').max().alias('session_end'),
        # Polars counts are unsigned unless explicitly cast to the contract type
        pl.len().cast(pl.Int64).alias('total_events'),
        pl.col('product_id').n_unique().cast(pl.Int64).alias('products_viewed'),
        (pl.col('event_type') == 'purchase').sum().cast(pl.Int64).alias('purchases'),
        pl.when(pl.col('event_type') == 'purchase')
          .then(pl.col('price'))
          .otherwise(0)
          .sum()
          .cast(pl.Decimal(38, 2))
          .alias('session_revenue')
    ])

    return result.to_arrow()


# ============================================================================
# Stage 3: Daily summary (final output)
# ============================================================================

@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def daily_summary(
    sessions: Annotated[
        pa.Table,
        bauplan.Model('session_metrics', projection_schema=DailySessionColumns),
    ],
) -> Annotated[pa.Table, DailySummarySchema]:
    """
    Computes daily metrics across all sessions; conversion is the percentage with at least one purchase.

    | date       | total_sessions | total_purchases | conversion_rate | total_revenue | avg_session_revenue |
    |------------|----------------|-----------------|-----------------|---------------|---------------------|
    | 2024-01-01 | 500            | 150             | 30.00           | 15000.00      | 30.00               |
    """
    import polars as pl  # ty: ignore[unresolved-import]

    df = pl.DataFrame(sessions)

    result = df.group_by(
        pl.col('session_start').dt.truncate('1d').alias('date')
    ).agg([
        pl.len().cast(pl.Int64).alias('total_sessions'),
        pl.col('purchases').sum().alias('total_purchases'),
        # A session converts once even when it contains multiple purchases
        ((pl.col('purchases') > 0).mean() * 100).round(2).alias('conversion_rate'),
        pl.col('session_revenue').sum().cast(pl.Decimal(38, 2)).alias('total_revenue'),
        pl.col('session_revenue').cast(pl.Float64).mean().round(2).alias('avg_session_revenue')
    ]).sort('date')

    return result.to_arrow()
```

### expectations.py

Expectations validate data quality after the pipeline runs. They must return `True` (pass) or raise an exception (fail).
```python
from typing import Annotated

import bauplan
import pyarrow as pa
from bauplan.standard_expectations import expect_column_no_nulls


@bauplan.expectation()
@bauplan.python('3.11')
def test_staging_completeness(
    data: Annotated[pa.Table, bauplan.Model('staging')],
) -> bool:
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

# 2. Create a branch to run
bauplan checkout -b <username>.<branch_name>

# 3. Dry run
bauplan run --dry-run --strict

# 4. Execute pipeline
bauplan run --strict

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
