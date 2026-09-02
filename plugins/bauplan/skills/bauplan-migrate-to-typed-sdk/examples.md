# Migration Patterns: Old SDK (0.1.x and 0.2.x) to Typed SDK (0.3.0+)

Each pattern below shows old-style code and its typed equivalent. Apply them in the order they appear when rewriting a file. All snippets come from real migrations of the official Bauplan examples.

## 1. Imports

Every migrated pipeline file gets this header shape:

```python
from typing import Annotated

import bauplan
import pyarrow
from bauplan import (
    Float64,
    Int64,
    Model,
    Parameter,
    String,
    TableField,
    TableSchema,
)
```

- `import bauplan` stays: the decorators are still used as `@bauplan.model`, `@bauplan.python`, `@bauplan.expectation`.
- `import pyarrow` is now required in every model file, even if the body never calls PyArrow directly, because input and output annotations name `pyarrow.Table`. `import pyarrow as pa` also works; the alias must match the annotations.
- The flat `from bauplan import (...)` covers the typing symbols. Fully qualified `bauplan.TableSchema` / `bauplan.Int64` is equally valid; pick one style per file and stay consistent.

## 2. Inputs: default argument to `Annotated`

Old:

```python
def taxi_trip_waiting_times(
    data=bauplan.Model("normalized_taxi_trips"),
):
```

New:

```python
def taxi_trip_waiting_times(
    data: Annotated[pyarrow.Table, Model("normalized_taxi_trips")],
):
```

- The hybrid form `data: pa.Table = bauplan.Model(...)` must also be migrated: move the `Model(...)` into the annotation and drop the default.
- The parameter name is free; the DAG edge comes from the table identifier inside `Model("name")`, not from the parameter name.
- `Model` still accepts the identifier positionally or as `name=`, and still resolves bare names vs `namespace.name` the same way.

## 3. Input `columns=[...]` to `projection_schema=`

The `columns` keyword no longer exists on `Model`. Column projection is now declared as a `TableSchema` class, which requires the column types (see pattern 5 for how to find them).

Old:

```python
    data: pa.Table = bauplan.Model(
        "transactions",
        columns=["account_id", "amount", "status", "txn_ts"],
        filter="txn_ts >= $start_date AND txn_ts < $end_date",
    ),
```

New:

```python
class TransactionColumns(TableSchema):
    """The projection of transactions needed to measure settled spend."""

    account_id: String
    amount: Float64
    status: String
    txn_ts: TimestampMicro


def account_summary(
    data: Annotated[
        pa.Table,
        Model(
            "transactions",
            projection_schema=TransactionColumns,
            filter="txn_ts >= $start_date AND txn_ts < $end_date",
        ),
    ],
) -> ...:
```

- `filter=` is unchanged: still a SQL-like string (compatible with Apache Datafusion), still supports `$param` templating. The new stub types it as `LiteralString`, so it must stay a literal (no f-strings, no variables).
- One projection schema class per input. Distinct class names within a file; collisions across files are fine.
- The new `Model` is a dataclass with only `name`, `projection_schema`, `filter`. The old kwargs `ref`, `connector`, `connector_config_key`, `connector_config_uri` no longer exist and have no equivalent: a model using them cannot be migrated mechanically. Stop and report it.

## 4. Decorator `columns=[...]` to a return annotation

The `columns` keyword no longer exists on `@bauplan.model()`. The output schema is now declared in the function's return annotation.

Old:

```python
@bauplan.model(
    columns=["date", "customer_segment", "total_revenue"],
    materialization_strategy="REPLACE",
)
def daily_segment_stats(...):
```

New:

```python
class DailySegmentStats(TableSchema):
    """Daily purchase statistics per customer segment."""

    date: String
    customer_segment: String
    total_revenue: Float64


@bauplan.model(materialization_strategy="REPLACE")
def daily_segment_stats(...) -> Annotated[pyarrow.Table, DailySegmentStats]:
```

- Every migrated model gets a return annotation, including models that declared no `columns` at all in the old code. This is the core of the migration: the return annotation is the model's schema contract.
- The old wildcard `columns=['*']` has no equivalent: enumerate the actual output columns.
- All other `@bauplan.model()` kwargs are unchanged: `name`, `materialization_strategy`, `cache_strategy`, `partitioned_by`, `overwrite_filter`, `internet_access`.
- The declared output schema restricts the columns the function may output and enforces the availability and type of each declared field (when type contracts are enabled on the run).

## 5. Declaring `TableSchema` classes

```python
class SurvivalColumns(bauplan.TableSchema):
    """A projection of 2 columns for survival rate analysis."""

    Age: Annotated[
        bauplan.Float64,
        bauplan.TableField(
            doc="A passenger's age in years.",
            lineage="titanic['Age']",
        ),
    ]
    Survived: bauplan.Int64
```

Rules:

- Schema classes must inherit directly from `TableSchema`. Any other base silently fails registration and the run errors with `returns unknown schema "<Name>"`.
- Give every schema class a one-line docstring saying what the projection or output represents.
- A bare type (`Survived: Int64`) is enough when there is no metadata. Use `Annotated[Type, TableField(...)]` to attach `doc`, `title`, `lineage`, or `nullable`.
- Available field types: `Bool`, `Int32`, `Int64`, `Float64`, `Decimal128`, `String`, `Binary`, `Date32`, `Date64`, `TimestampMicro`, `TimestampNano`, `TimestampMicroUTC`, `TimestampNanoUTC`. There is no unsigned or 32-bit float type; map `float`/`double` source columns to `Float64` and cast unsigned integers to a signed type in the model body (see pattern 9).
- Mapping from the `TYPE` column printed by `bauplan table get`:

| SDK type | Arrow type | Iceberg type (catalog) |
|---|---|---|
| Bool | bool | boolean |
| Int32 | int32 | int |
| Int64 | int64 | long |
| Float64 | float64 | double |
| Decimal128[p, s] | decimal128(p, s) | decimal(p, s), p ≤ 38 |
| String | string (utf8) | string |
| Date32 | date32 (days) | date |
| Date64 | date64 (milliseconds) | date (Iceberg has day precision only) |
| TimestampMicro | timestamp('us') | timestamp |
| TimestampNano | timestamp('ns') | timestamp_ns (Iceberg v3 only) |
| TimestampMicroUTC | timestamp('us', tz='UTC') | timestamptz |
| TimestampNanoUTC | timestamp('ns', tz='UTC') | timestamptz_ns (Iceberg v3 only) |
| Binary | binary | binary |

- Nested types (`list<...>`, `struct<...>`, `map<...>`) have no field type. A model whose output contains a nested column (e.g. an `array_agg` result, an embeddings vector) keeps no return annotation at all; annotations are opt-in per model, so the rest of the pipeline can still be typed. Flag these models in the final report.
- Parameterized types use subscript syntax, never a call: `Decimal128[4, 2]` (precision, scale). `Decimal128(4, 2)` is invalid.
- `lineage` documents where a field comes from. Reference a catalog table with the string form `lineage="titanic['Age']"`, or another schema class in the same file with a live reference `lineage=TripColumns['pickup_datetime']`. When the migrated field's type differs from its source column, drop the `lineage` rather than declaring a conflicting one.

## 6. Expectations

Old:

```python
@bauplan.expectation()
@bauplan.python("3.11")
def test_txn_id_unique(
    data=bauplan.Model("transactions", columns=["txn_id"]),
):
    """txn_id is the primary key"""
    return expect_column_all_unique(data, "txn_id")
```

New:

```python
class TxnId(TableSchema):
    """The primary key of the transaction stream."""

    txn_id: String


@bauplan.expectation()
@bauplan.python("3.11")
def test_txn_id_unique(
    data: Annotated[pyarrow.Table, Model("transactions", projection_schema=TxnId)],
) -> bool:
    """txn_id is the primary key"""
    return expect_column_all_unique(data, "txn_id")
```

- Expectations are annotated `-> bool`, never with an output `TableSchema`.
- `@bauplan.expectation()` itself and `from bauplan.standard_expectations import ...` are unchanged.
- Expectations that only assert a column's dtype (e.g. `data.schema.field("Fare").type == pa.float32()`) become redundant once the schema is declared in a type annotation. Flag them to the user and propose replacing them with a value-level check (ranges, uniqueness, nulls); do not delete them silently.

## 7. Parameters

Old:

```python
    inflation_rate=bauplan.Parameter("inflation_rate"),
    run_id=bauplan.Parameter("run_id"),
```

New:

```python
    inflation_rate: Annotated[float, Parameter("inflation_rate")],
    run_id: Annotated[int, Parameter("run_id")],
```

- The annotation carries the plain Python type of the value (`float`, `int`, `str`, `bool`), matching the parameter's `type:` in `bauplan_project.yml`. Do not use Bauplan field types like `Float64` here.
- `bauplan_project.yml` itself does not change. `$param` interpolation inside `filter=` and `overwrite_filter=` is unchanged.

## 8. Return values must be a `pyarrow.Table`

The old runtime accepted an Arrow table, a Polars or pandas DataFrame, or a list of dicts as a model's return value. With a `-> Annotated[pyarrow.Table, Schema]` contract, return an actual `pyarrow.Table`:

```python
    return pyarrow.Table.from_pylist(rows)          # was: return rows (list of dicts)
    return pyarrow.Table.from_pandas(df)            # was: return df (pandas)
    return relation.to_arrow_table()                # was: relation.arrow() (duckdb)
    return df.to_arrow()                            # polars: unchanged
```

When converting pandas, select frames, not Series: `df[["col"]]`, not `df["col"]`.

## 9. Dtype tightening

Declared schemas pin exact types, so transformations that silently produced a different dtype now need an explicit cast. The most common case is Polars `pl.len()`, which yields `UInt32` while Iceberg has no unsigned types:

```python
    pl.len().cast(pl.Int64).alias("transaction_count"),
    (pl.col("event_type") == "login").sum().cast(pl.Int64).alias("login_count"),
```

These mismatches do not show up in a dry run; they surface on a real run against data. Always finish validation with a full run on an isolated branch.

## 10. SQL models

SQL models declare their output schema with a directive comment referencing a `TableSchema` class defined in the sibling `models.py`:

```sql
-- bauplan: materialization_strategy = NONE
-- bauplan: output_schema = TaxiModelSchema
SELECT pickup_datetime, PULocationID FROM taxi_fhvhv
```

## 11. Complete before and after

Old-style project (the pre-0.3 `bauplan init` scaffold):

```python
import bauplan
from bauplan.standard_expectations import expect_column_all_unique


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model()
def survival_rate_by_age(
    passengers=bauplan.Model(
        "titanic", columns=["Age", "Survived"], filter="Age IS NOT NULL"
    ),
):
    """Bins passengers by age and returns survival rate per bin."""
    import polars as pl

    df = pl.DataFrame(passengers)
    return (
        df.group_by(pl.col("Age").floor())
        .agg(pl.col("Survived").mean().alias("survival_rate"))
        .sort("Age")
        .to_arrow()
    )


@bauplan.expectation()
@bauplan.python("3.12")
def test_age(data=bauplan.Model("survival_rate_by_age", columns=["Age"])):
    """Validates that the Age bins are unique"""
    return expect_column_all_unique(data, "Age")
```

Migrated:

```python
from typing import Annotated

import bauplan
import pyarrow

from bauplan.standard_expectations import expect_column_all_unique


class SurvivalColumns(bauplan.TableSchema):
    """A projection of 2 columns for survival rate analysis."""

    Age: Annotated[
        bauplan.Float64,
        bauplan.TableField(
            doc="A passenger's age in years.",
            lineage="titanic['Age']",
        ),
    ]
    Survived: Annotated[
        bauplan.Int64,
        bauplan.TableField(
            doc="1 if the passenger survived, 0 otherwise.",
            lineage="titanic['Survived']",
        ),
    ]


class SurvivalRateSchema(bauplan.TableSchema):
    """Survival rate of passengers grouped by age (in years)."""

    Age: bauplan.Float64
    survival_rate: bauplan.Float64


@bauplan.python("3.12", pip={"polars": "1.37"})
@bauplan.model()
def survival_rate_by_age(
    passengers: Annotated[
        pyarrow.Table,
        bauplan.Model(
            "titanic",
            projection_schema=SurvivalColumns,
            filter="Age IS NOT NULL",
        ),
    ],
) -> Annotated[pyarrow.Table, SurvivalRateSchema]:
    """Bins passengers by age and returns survival rate per bin."""
    import polars as pl

    df = pl.DataFrame(passengers)
    return (
        df.group_by(pl.col("Age").floor())
        .agg(pl.col("Survived").mean().alias("survival_rate"))
        .sort("Age")
        .to_arrow()
    )


class AgeOnly(bauplan.TableSchema):
    """The column validated by `test_age`."""

    Age: bauplan.Float64


@bauplan.expectation()
@bauplan.python("3.12")
def test_age(
    data: Annotated[
        pyarrow.Table,
        bauplan.Model("survival_rate_by_age", projection_schema=AgeOnly),
    ],
) -> bool:
    """Validates that the Age bins are unique"""
    return expect_column_all_unique(data, "Age")
```

Note how the output schema declares what the function actually produces: `Age` stays `Float64` because `pl.col("Age").floor()` keeps the float dtype. Deriving the schema from the transformation, not from wishful thinking, is what makes the contract pass on a real run.

## 12. Do not over-migrate

Everything below is unchanged between 0.1.x/0.2.x and 0.3.0+. Leave it alone:

- `@bauplan.python(version, pip={...})`, `@bauplan.expectation()`, `@bauplan.resources()`, `@bauplan.extras()`
- `@bauplan.model()` kwargs other than `columns`: `name`, `materialization_strategy`, `cache_strategy`, `partitioned_by`, `overwrite_filter`, `internet_access`
- Decorator ordering (`@bauplan.model` above or below `@bauplan.python`, both valid)
- `filter=` syntax and `$param` templating
- `bauplan_project.yml` structure (no new keys required by the migration)
- `bauplan.standard_expectations` function names and usage
- Package imports inside model bodies (still required, one environment per model)
- DAG wiring semantics: edges by model identifier string, namespace resolution rules
- The `bauplan.Client` SDK surface (queries, branches, jobs)
