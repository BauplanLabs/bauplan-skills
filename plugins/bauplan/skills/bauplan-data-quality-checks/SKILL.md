---
name: bauplan-data-quality-checks
description: "Generates data quality check code for bauplan pipelines and ingestion workflows. Invoked by the bauplan-data-pipeline and bauplan-safe-ingestion skills, or directly by the user. Produces expectations.py for pipelines or validation logic for WAP scripts. Output is always code, never reports."
allowed-tools:
  - Bash(bauplan:*)
  - Read
  - Write
  - Glob
  - Grep
  - WebFetch(domain:docs.bauplanlabs.com)
---

# Data Quality Checks

This skill writes data quality check code. It produces one of two things:

1. **Pipeline expectations** — an `expectations.py` file using `@bauplan.expectation()` that runs as part of `bauplan run`.
2. **Ingestion validation** — a `validate_import()` function using the bauplan SDK, embedded in a WAP script between `import_data()` and `merge_branch()`.

Output is always working code. Not reports, not profiling summaries, not markdown.

This skill is invoked by the `bauplan-data-pipeline` and `bauplan-safe-ingestion` workflow skills when they determine that quality checks are needed. It can also be invoked directly by the user. Either way, the skill needs to know three things before it can write code:

1. **What table(s) and branch** — `namespace.table_name` and the ref to validate against
2. **What context** — pipeline or ingestion, which determines the code form
3. **What to check** — this comes in one of two forms:
   - **User specifications**: the user states checks directly ("user_id must be unique, age must be positive"). Translate to code.
   - **Pipeline code**: a `models.py` exists that consumes the table. Read it, derive checks, propose them to the user for confirmation, then write code.

If the skill is invoked without enough information, ask for what's missing. But ask for specifics — "which columns and what properties?" — not for a general description of the pipeline's purpose.

## CRITICAL: Branch Safety

> **NEVER run checks or pipelines on `main`.**
> All validation targets a development or import branch.

Branch naming convention: `<username>.<branch_name>`. Get your username with `bauplan info`.

## Environment Setup

Before writing any Python, check whether the project uses `uv` (look for `pyproject.toml` or `uv.lock`). If so, use `uv run python` to execute scripts and `uv add` to install packages. Otherwise, use the system `python` and `pip install`.

Ensure the required packages are installed:
- `bauplan` (the Bauplan Python SDK — required)
- `polars` (if custom expectations need DataFrame operations — zero-copy Arrow interop)

Ensure the environment has a typed SDK build (0.3.0+). Typed declarations are only available with SDK 0.3.0+: older versions are *not* compatible. Use that same environment for all CLI commands and verify connectivity with `bauplan info`. Models declare their own runtime dependencies via `@bauplan.python('3.11', pip={...})`; annotation imports require `pyarrow` locally too.

**Do not use pandas.** Bauplan's `client.query()` returns a PyArrow table directly: no `.to_arrow()` call needed. In pipeline expectations, model inputs arrive as Arrow tables too. Polars reads Arrow natively with zero-copy (`pl.DataFrame(table)`). Pandas requires a full data copy and is slower.

## Writing Effective Checks

This section is the foundation. It applies to both pipeline expectations and ingestion validation — the thinking is identical, only the code form differs.

When the user provides explicit specifications, much of this thinking is already done. Translate their specs to code, using the methodology below to fill in gaps (e.g., if they specify a check but not its severity).

When deriving checks from pipeline code, this methodology is the primary tool.

### Profiling Is Not Testing

**Profiling** is exploratory — you compute statistics to learn what the data looks like. You don't know the shape yet. Use the `bauplan-explore-data` skill for this.

**Testing** is confirmatory — you know what the data should look like and you verify that it does. Every check encodes a specific expectation that can pass or fail.

The workflow is: profile once to understand the data, form assumptions from what you learn, then encode those assumptions as tests using this skill. If the user hasn't profiled the data and can't state what they expect, they're not ready for this skill yet — point them to `bauplan-explore-data` first.

### Every Check Needs a Reason

For each column you check, state why you're checking it and what you expect. If you can't state why, drop the check. Ten focused checks with clear assertions beat a hundred random queries nobody reads.

The anti-pattern: running null counts, uniqueness checks, and range queries on every column in a table because they exist. This produces numbers, not insights. Nobody acts on "column X has 3.2% nulls" unless they know whether 3.2% is acceptable.

### State Assumptions Before Writing Code

Every check encodes a hypothesis. Before writing any expectation or validation query, state it:

> "I expect **[column]** to be **[property]** because **[consumer/reason]**, and if it fails it should **[halt/warn]** because **[impact]**."

Examples:

- "I expect `order_id` to have no nulls because the billing pipeline joins on it, and if it fails it should halt because every downstream table breaks."
- "I expect `event_time` to be within the last 24 hours because the dashboard shows daily metrics, and if it fails it should warn because stale data is misleading but not corrupting."
- "I expect `price` to be positive because the revenue model sums it, and if it fails it should halt because negative prices produce wrong totals."

**If you cannot fill in this template for a check, you do not have enough context to write it.** Ask the user, inspect the schema, or read the downstream models.

When the user provides specifications directly, the "because" clause may be implicit. That's fine — they've done the reasoning. But if they haven't specified severity (FAIL vs WARN), use the template to figure it out.

### Deriving Checks from Pipeline Code

When a `models.py` exists, find `bauplan.Model()` inside `Annotated[pa.Table, ...]` parameters and read each referenced projection and output `TableSchema`.

- **`projection_schema` fields** → those columns are needed downstream. Check completeness on critical ones. If no projection is declared, inspect the body to identify columns used.
- **Output schemas and `TableField` metadata** → declared types, nullability (`Type | None` for optional, a bare type for required), documentation, and lineage describe the contract. Use them alongside the transformation to derive value-level checks; a declared type alone does not prove uniqueness or valid ranges.
- **`filter` expressions** → the model assumes data matching this condition. Check that the assumption holds (e.g., `filter="total > 0"` → check for non-positive values).
- **Joins** (multiple `bauplan.Model()` inputs joined on a key) → the join column needs uniqueness in the parent table and no nulls in both.
- **Arithmetic in the model body** (divisions, sums, averages) → denominators need non-zero checks, aggregated columns need non-null checks.

Example: given this model input, with string identifiers and a double-precision total confirmed from the source schema:

```python
from typing import Annotated

import bauplan
import pyarrow as pa


class OrderCheckColumns(bauplan.TableSchema):
    """Order fields used by the downstream consumer."""

    order_id: bauplan.String | None
    total: bauplan.Float64 | None
    customer_id: bauplan.String | None


# Input parameter declaration inside the consuming model's signature
data: Annotated[
    pa.Table,
    bauplan.Model('orders', projection_schema=OrderCheckColumns, filter="total > 0"),
]
```

Derive:
- `order_id`: selected column, likely a key → no nulls (FAIL), unique (FAIL)
- `total`: filtered to positive → no nulls (FAIL), all values > 0 (FAIL)
- `customer_id`: selected column → no nulls (FAIL); if joined to a customers table → unique in that table

**Always propose derived checks to the user for confirmation before writing code.** State each as a plain-language assumption and let the user approve, modify, or remove.

### Organize Checks by Quality Dimension

Six dimensions, each producing a specific kind of assertion:

**Completeness** — are required values present?
- Null rates on critical columns
- Missing rows or gaps in time series
- Built-in: `expect_column_no_nulls`, `expect_column_some_null`, `expect_column_all_null`

**Uniqueness** — are identifiers actually unique?
- Primary key uniqueness
- Duplicate record detection
- Built-in: `expect_column_all_unique`, `expect_column_not_unique`

**Validity** — do values conform to expected format and range?
- Numeric bounds (min, max, mean)
- Allowed values for categorical columns
- Data type conformance through pipeline schema contracts; explicit schema checks remain relevant for ingestion
- Built-in: `expect_column_accepted_values`, `expect_column_mean_greater_than`, `expect_column_mean_smaller_than`, `expect_column_mean_greater_or_equal_than`, `expect_column_mean_smaller_or_equal_than`

**Freshness** — is the data current enough for its purpose?
- Most recent timestamp vs. expected cadence
- Staleness relative to current time or a deadline
- No built-in; write custom checks

**Consistency** — do related values agree?
- Cross-column rules (end_date >= start_date)
- Referential integrity (foreign keys exist in parent table)
- Column derivation rules
- Built-in: `expect_column_equal_concatenation`; custom for most cases

**Volume** — is the amount of data within expected bounds?
- Row count vs. expected range
- Sudden drops or spikes relative to prior loads
- No built-in; write custom checks

Not every table needs all six dimensions. Pick the ones that matter for this table's consumers.

### Classify Every Check by Severity

Every check must have a severity before you write the code. This determines whether it halts execution or logs a warning.

**FAIL** (halt the pipeline / block the merge):
Downstream use is unsafe. Examples: missing primary key, zero rows, wrong schema, broken join column, negative values in a revenue column that gets summed.

In pipeline expectations: use `assert`.
In ingestion validation: raise an exception to prevent `merge_branch()`.

**WARN** (log and continue):
Quality is degraded but not catastrophic. Examples: higher-than-usual null rate in a non-critical column, data is 2 hours stale against a 24-hour SLA, unexpected but non-breaking extra columns.

In pipeline expectations: print the result, do not assert.
In ingestion validation: log the warning, let the user decide whether to merge.

**If you cannot classify a check, you do not understand its impact yet.** Go back to the consumer analysis.

### Pin Checks to a Data State

Every check must run against a specific branch and ref. Never check "whatever main looks like right now."

- In pipelines: `bauplan run` handles this — the run executes against the checked-out branch.
- In ingestion: all `client.query()` calls use `ref=branch_name` explicitly.

This ensures checks are reproducible. If a check fails, you can go back to exactly that data state and investigate.

## Pipeline Expectations

### Where They Live

```text
my-pipeline/
  bauplan_project.yml
  models.py
  expectations.py        ← quality checks live here
```

Expectations are Python functions in `expectations.py` in the pipeline project directory, alongside `models.py`.

### How Expectations Work

An expectation is a function decorated with `@bauplan.expectation()` that takes one or more tables via `Annotated[pa.Table, bauplan.Model(...)]` parameters and declares `-> bool`.

```python
from typing import Annotated

import bauplan
import pyarrow as pa


class OrderIdCheckColumns(bauplan.TableSchema):
    """Order identifier checked for completeness."""

    order_id: bauplan.String | None

@bauplan.expectation()
@bauplan.python('3.11')
def test_no_null_order_ids(
    data: Annotated[
        pa.Table, bauplan.Model('clean_orders', projection_schema=OrderIdCheckColumns)
    ],
) -> bool:
    """order_id must not be null because billing joins on it."""
    from bauplan.standard_expectations import expect_column_no_nulls
    result = expect_column_no_nulls(data, 'order_id')
    assert result, 'order_id contains null values'
    return result
```

Key mechanics:
- Read column types from catalog metadata or the producing model's output schema before declaring projection fields. Never infer types from names alone. Schema classes inherit directly from `TableSchema`, have docstrings, and use names unique across project files.
- `Model` accepts only `name`, `projection_schema`, and literal `filter`; `$param` templating is supported. Run parameters use `Annotated[<Python type>, bauplan.Parameter("name")]`, matching the YAML parameter type.
- Expectations run as DAG nodes during `bauplan run`, after the model they depend on completes.
- They receive the model's output as an Arrow table — same as a downstream model would.
- `True` = pass, `False` = fail.
- `assert` makes the failure halt the pipeline, whatever the run options are.
- Strict mode is on by default, so returning `False` without an `assert` also fails the run. `bauplan run --no-strict` is what turns those into logged failures that let the run finish.

### Using bauplan.standard_expectations

The built-in library provides vectorized, SIMD-optimized checks. **Always prefer these over hand-rolled equivalents** — they are faster, more memory-efficient, and easier to maintain.

Each function takes an Arrow table and returns a boolean:

| Function                                                      | Dimension    | What it checks                        |
|---------------------------------------------------------------|--------------|---------------------------------------|
| `expect_column_no_nulls(table, col)`                          | Completeness | Column has zero null values           |
| `expect_column_some_null(table, col)`                         | Completeness | Column has at least one null          |
| `expect_column_all_null(table, col)`                          | Completeness | Column is entirely null               |
| `expect_column_all_unique(table, col)`                        | Uniqueness   | All values are distinct               |
| `expect_column_not_unique(table, col)`                        | Uniqueness   | Column has at least one duplicate     |
| `expect_column_accepted_values(table, col, values)`           | Validity     | All values are in allowed set         |
| `expect_column_mean_greater_than(table, col, val)`            | Validity     | Mean exceeds threshold                |
| `expect_column_mean_smaller_than(table, col, val)`            | Validity     | Mean below threshold                  |
| `expect_column_mean_greater_or_equal_than(table, col, val)`   | Validity     | Mean >= threshold                     |
| `expect_column_mean_smaller_or_equal_than(table, col, val)`   | Validity     | Mean <= threshold                     |
| `expect_column_equal_concatenation(table, target, cols, sep)` | Consistency  | Column equals concatenation of others |

Import them inside the function body, not at module level:

```python
class EventTypeCheckColumns(bauplan.TableSchema):
    """Event category checked against accepted values."""

    event_type: bauplan.String | None


@bauplan.expectation()
@bauplan.python('3.11')
def test_valid_event_types(
    data: Annotated[
        pa.Table, bauplan.Model('staging', projection_schema=EventTypeCheckColumns)
    ],
) -> bool:
    """event_type must be known because downstream filters depend on it."""
    from bauplan.standard_expectations import expect_column_accepted_values
    result = expect_column_accepted_values(
        data, 'event_type', ['view', 'cart', 'purchase', 'remove']
    )
    assert result, 'event_type contains unexpected values'
    return result
```

### Writing Custom Expectations

For checks not covered by the built-in library — freshness, volume, cross-column logic, referential integrity — write custom expectations using the Arrow table directly or with Polars/DuckDB.

**Freshness check:**
```python
class SummaryDateCheckColumns(bauplan.TableSchema):
    """Summary timestamp checked for freshness."""

    date: bauplan.TimestampMicro | None


@bauplan.expectation()
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def test_data_freshness(
    data: Annotated[
        pa.Table, bauplan.Model('daily_summary', projection_schema=SummaryDateCheckColumns)
    ],
) -> bool:
    """Most recent date must be within 2 days for the daily dashboard."""
    import polars as pl
    from datetime import datetime, timedelta

    df = pl.DataFrame(data)
    max_date = df.select(pl.col('date').max()).item()
    threshold = datetime.now() - timedelta(days=2)
    is_fresh = max_date >= threshold
    assert is_fresh, f'Data is stale: most recent date is {max_date}'
    return is_fresh
```

**Volume check:**
```python
@bauplan.expectation()
@bauplan.python('3.11')
def test_minimum_row_count(
    data: Annotated[pa.Table, bauplan.Model('staging')],
) -> bool:
    """Table must have at least 1000 rows to meet the expected source volume."""
    row_count = data.num_rows
    is_sufficient = row_count >= 1000
    assert is_sufficient, f'Only {row_count} rows — expected at least 1000'
    return is_sufficient
```

**Cross-column consistency:**
```python
class TripTimeCheckColumns(bauplan.TableSchema):
    """Trip timestamps checked for chronological consistency."""

    pickup_datetime: bauplan.TimestampMicro | None
    dropoff_datetime: bauplan.TimestampMicro | None


@bauplan.expectation()
@bauplan.python('3.11', pip={'polars': '1.15.0'})
def test_dates_ordered(
    data: Annotated[
        pa.Table, bauplan.Model('trips', projection_schema=TripTimeCheckColumns)
    ],
) -> bool:
    """dropoff must be after pickup for valid trip durations."""
    import polars as pl

    df = pl.DataFrame(data)
    violations = df.filter(pl.col('dropoff_datetime') < pl.col('pickup_datetime'))
    is_valid = violations.height == 0
    assert is_valid, f'{violations.height} rows have dropoff before pickup'
    return is_valid
```

### Output Column Validation

Models declare output contracts with `-> Annotated[pa.Table, OutputSchema]`. The return annotation is mandatory and must take this exact form: a bare `-> pa.Table` and a missing annotation are both rejected at parse time. The schema declares columns and their types, with `Type | None` for an optional column and a bare type for a required one. A required output column is enforced by counting nulls in the returned data, so it doubles as a not-null check and makes a separate no-nulls expectation on that column redundant. This example assumes string IDs, double-precision totals, and day-precision dates:

```python
class CleanOrdersSchema(bauplan.TableSchema):
    """Order fields retained for downstream consumers."""

    order_id: bauplan.String | None
    customer_id: bauplan.String | None
    total: bauplan.Float64 | None
    order_date: bauplan.Date32 | None


@bauplan.model(materialization_strategy='REPLACE')
@bauplan.python('3.11')
def clean_orders(
    data: Annotated[
        pa.Table, bauplan.Model('raw_orders', projection_schema=CleanOrdersSchema)
    ],
) -> Annotated[pa.Table, CleanOrdersSchema]:
    """Retain the order fields required downstream."""
    return data
```

Output schemas are exhaustive and enforced on every run with no flag to opt in: the returned table must carry exactly the declared columns, so a missing column, an extra one, or a dtype mismatch fails the run with `ModelOutputContractError`. Value-level expectations still check ranges, uniqueness, freshness, and business rules. Flag existing dtype-only expectations as potentially redundant when the corresponding contract covers them; propose replacements without deleting checks silently. Decimal columns and nested output fields have no supported schema type and are annotated `bauplan.Any`, so the contract does not constrain them and they still need explicit checks.

SQL models use `-- bauplan: output_schema = SchemaName` referencing a `TableSchema` in the sibling `models.py`.

### Running and Verifying

Type check first: the annotations on expectations and projection schemas make `uv run ty check` (or `ty check`) a local gate that catches misspelled field types, unknown `bauplan` symbols, and schema classes that do not resolve, in seconds and without submitting a job. Imports of model runtime dependencies (polars, duckdb) resolve only in the remote environment, so add them to the project's dev dependencies (`uv add --dev polars`) to keep the report clean. Only then go to the platform.

```bash
# First gate: annotations are checked locally, before any job is submitted
uv run ty check

# Validate declarations and schema references without materializing
bauplan run --dry-run

# Execute with blocking expectations and output contract validation
bauplan run
```

After a run, expectation results appear in the run output. Failed expectations show the assertion message. Use `bauplan job logs <job_id>` to review results from a previous run.

## Ingestion Validation

### Where It Lives

Validation logic is embedded directly in the WAP (Write-Audit-Publish) script, between `import_data()` and the merge decision. No separate file, no decorators — SDK calls and assertions in Python.

```python
# === IMPORT PHASE ===
client.import_data(table=table_name, search_uri=s3_path, branch=branch_name, ...)

# === VALIDATION PHASE ===    ← checks go here
validate_import(client, table_name, branch_name, namespace)

# === MERGE PHASE ===
client.merge_branch(source_ref=branch_name, into_branch="main")
```

### Extending an Existing Script

If the user already has a working ingestion script and wants to add or strengthen validation:

1. Read the script to find the current validate phase — look for the code between `import_data()` and `merge_branch()`.
2. Note what checks already exist (typically just a row count).
3. Generate a `validate_import()` function using either the user's specifications or checks derived from pipeline code.
4. Replace the existing validate phase with a call to `validate_import()`.

Do not rewrite the import or merge phases. Only touch the validation logic.

### Validation Structure

Write a validation function that runs all checks and raises on failure:

```python
def validate_import(client, table_name, branch, namespace="bauplan"):
    fq_table = f"{namespace}.{table_name}"

    # FAIL checks — assert to block merge
    result = client.query(f"SELECT COUNT(*) as n FROM {fq_table}", ref=branch)
    row_count = result.column("n")[0].as_py()
    assert row_count > 0, f"{fq_table} has 0 rows after import"

    # WARN checks — print, don't assert
    result = client.query(f"SELECT MIN(total) as lo FROM {fq_table}", ref=branch)
    if result.column("lo")[0].as_py() < 0:
        print(f"⚠ negative totals found")

    # ... see examples/ingestion_validation.py for complete implementation
```

### Key Differences from Pipeline Expectations

| Aspect           | Pipeline (`expectations.py`)      | Ingestion (WAP script)           |
|------------------|-----------------------------------|----------------------------------|
| Decorator        | `@bauplan.expectation()`          | None — plain Python              |
| Input            | Arrow table via `bauplan.Model()` | SDK queries via `client.query()` |
| Execution        | Automatic during `bauplan run`    | Called explicitly in script      |
| Data access      | In-memory Arrow table             | SQL queries against branch       |
| Failure handling | `assert` halts pipeline           | `assert` prevents merge          |
| Environment      | Containerized per-function        | Local Python process             |

### Query Patterns by Dimension

All queries must use `ref=branch_name` to pin to the import branch.

**Volume:**
```python
result = client.query(f"SELECT COUNT(*) as n FROM {fq_table}", ref=branch)
row_count = result.column("n")[0].as_py()
```

**Schema:**
```python
table_meta = client.get_table(table=table_name, ref=branch, namespace=namespace)
actual_columns = {f.name for f in table_meta.fields}
actual_types = {f.name: f.type for f in table_meta.fields}
```

**Completeness (null count):**
```python
result = client.query(
    f"SELECT COUNT(*) - COUNT({col}) as nulls FROM {fq_table}", ref=branch
)
```

**Uniqueness (duplicate count):**
```python
result = client.query(
    f"SELECT COUNT({col}) - COUNT(DISTINCT {col}) as dupes FROM {fq_table}", ref=branch
)
```

**Validity (bounds):**
```python
result = client.query(
    f"SELECT MIN({col}) as lo, MAX({col}) as hi FROM {fq_table}", ref=branch
)
```

**Freshness:**
```python
result = client.query(
    f"SELECT MAX({time_col}) as latest FROM {fq_table}", ref=branch
)
```

**Consistency (cross-column):**
```python
result = client.query(
    f"SELECT COUNT(*) as violations FROM {fq_table} WHERE {col_a} > {col_b}",
    ref=branch
)
```

## Reference

When unsure about a method signature, CLI flag, or concept, fetch the relevant doc page via `WebFetch` rather than guessing. Pages are markdown and LLM-friendly.

**Python SDK:** `https://docs.bauplanlabs.com/reference/bauplan.md`
**Standard expectations:** `https://docs.bauplanlabs.com/reference/bauplan-standard-expectations.md`

**Relevant concept pages:**
- Semantic annotations: `https://docs.bauplanlabs.com/concepts/semantic_annotations.md`
- Expectations: `https://docs.bauplanlabs.com/concepts/expectations.md`

**Full doc index:** `https://docs.bauplanlabs.com/llms.txt`

**CLI:** The `bauplan` CLI is self-documenting:
- `bauplan --help` — lists all available commands
- `bauplan <command> --help` — shows arguments and options for a specific command (e.g., `bauplan run --help`, `bauplan job --help`)

**Validating generated Python:** After writing or updating `expectations.py` or validation code, run `ruff check` and `ruff format` to catch syntax errors and style issues, and `uv run ty check` (or `ty check`) to catch type errors: these verify the code compiles and the annotations and SDK calls are well-formed without executing it. Only run these if they are installed (check with `which ruff` / `which ty`).
