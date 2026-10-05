# DAG optimization patterns

## Contents

[0. Imports](#0-imports) · [1. One function to named steps](#1-one-function-to-named-steps) · [2. Materialize by consumption](#2-materialize-by-consumption) · [3. Projections and catalog filters](#3-projections-and-catalog-filters) · [4. One shared preparation](#4-one-shared-preparation) · [5. Parent computes, children derive](#5-parent-computes-children-derive) · [6. Merge a whole-table handoff](#6-merge-a-whole-table-handoff) · [7. Partition for read pruning](#7-partition-for-read-pruning) · [8. Incremental output](#8-incremental-output)

The examples use the typed SDK (0.3.0+) and generic table names. Pin `python` and `pip` versions to the project's lock file. The column types are illustrative: take the real ones from `bauplan table get <namespace>.<table>`, and see the `bauplan-migrate-to-typed-sdk` skill for the full type mapping and schema rules.

## 0. Imports

Every snippet below assumes this header:

```python
from typing import Annotated

import bauplan
import pyarrow as pa
from bauplan import Date32, Float64, Model, String, TableSchema, TimestampMicro
```

## 1. One function to named steps

Before: a single model does everything, reads its inputs whole, and every intermediate result exists only inside it.

```python
class MonthlyBilling(TableSchema):
    """Billed amount per account and month."""

    account_id: String | None
    month: TimestampMicro | None
    amount: Float64 | None


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.12", pip={"polars": "1.42.1"})
def monthly_billing(
    transactions: Annotated[pa.Table, Model("raw.transactions")],
    accounts: Annotated[pa.Table, Model("raw.accounts")],
) -> Annotated[pa.Table, MonthlyBilling]:
    """Billed amount per account and month."""
    import polars as pl

    tx = pl.DataFrame(transactions).with_columns(pl.col("account_id").str.to_uppercase())
    acc = pl.DataFrame(accounts).with_columns(pl.col("account_id").str.to_uppercase())
    lines = tx.join(acc, on="account_id", how="left")
    return (
        lines.group_by("account_id", pl.col("posted_at").dt.month_start().alias("month"))
        .agg(pl.col("amount").sum())
        .to_arrow()
    )
```

After: one model per table transformation, a projection on every catalog input, and an output schema per model. Only the final output is materialized.

```python
class TransactionColumns(TableSchema):
    """The transaction columns needed for billing."""

    account_id: String | None
    amount: Float64 | None
    posted_at: TimestampMicro | None


class PreparedTransactions(TableSchema):
    """Transactions with a normalized account key."""

    account_id: String | None
    amount: Float64 | None
    posted_at: TimestampMicro | None


class AccountColumns(TableSchema):
    """The account columns needed for billing."""

    account_id: String | None
    segment: String | None


class PreparedAccounts(TableSchema):
    """Accounts with a normalized account key."""

    account_id: String | None
    segment: String | None


class BillingLines(TableSchema):
    """Transactions with their account attributes."""

    account_id: String | None
    amount: Float64 | None
    posted_at: TimestampMicro | None
    segment: String | None


@bauplan.model()
@bauplan.python("3.12", pip={"polars": "1.42.1"})
def prepared_transactions(
    transactions: Annotated[pa.Table, Model("raw.transactions", projection_schema=TransactionColumns)],
) -> Annotated[pa.Table, PreparedTransactions]:
    """Normalize the account key of every transaction."""
    import polars as pl

    return pl.DataFrame(transactions).with_columns(pl.col("account_id").str.to_uppercase()).to_arrow()


@bauplan.model()
@bauplan.python("3.12", pip={"polars": "1.42.1"})
def prepared_accounts(
    accounts: Annotated[pa.Table, Model("raw.accounts", projection_schema=AccountColumns)],
) -> Annotated[pa.Table, PreparedAccounts]:
    """Normalize the account key of every account."""
    import polars as pl

    return pl.DataFrame(accounts).with_columns(pl.col("account_id").str.to_uppercase()).to_arrow()


@bauplan.model()
@bauplan.python("3.12", pip={"polars": "1.42.1"})
def billing_lines(
    transactions: Annotated[pa.Table, Model("prepared_transactions")],
    accounts: Annotated[pa.Table, Model("prepared_accounts")],
) -> Annotated[pa.Table, BillingLines]:
    """Attach account attributes to every transaction."""
    import polars as pl

    return pl.DataFrame(transactions).join(pl.DataFrame(accounts), on="account_id", how="left").to_arrow()


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.12", pip={"polars": "1.42.1"})
def monthly_billing(
    lines: Annotated[pa.Table, Model("billing_lines")],
) -> Annotated[pa.Table, MonthlyBilling]:
    """Billed amount per account and month."""
    import polars as pl

    return (
        pl.DataFrame(lines)
        .group_by("account_id", pl.col("posted_at").dt.month_start().alias("month"))
        .agg(pl.col("amount").sum())
        .to_arrow()
    )
```

The output schemas are exhaustive: each model must return exactly the declared columns and types. When a model stops carrying a column, remove it from its schema as well.

## 2. Materialize by consumption

Before, in a larger DAG than pattern 1, where `prepared_accounts` is also read by three other projects: every model has `materialization_strategy="REPLACE"`, so each is written to the lake and read back by its child.

After: keep it only where a reason exists.

| Model | Read by | Materialize |
| --- | --- | --- |
| `prepared_transactions` | `billing_lines` only | No |
| `billing_lines` | `monthly_billing` only | No |
| `monthly_billing` | dashboard, another project | Yes |
| `prepared_accounts` | three other projects | Yes |

## 3. Projections and catalog filters

```python
class EventColumns(TableSchema):
    """The event columns needed for the reporting window."""

    event_id: String | None
    account_id: String | None
    event_type: String | None
    event_date: Date32 | None


class RecentEvents(TableSchema):
    """Events of the reporting window."""

    event_id: String | None
    account_id: String | None
    event_type: String | None
    event_date: Date32 | None


@bauplan.model()
@bauplan.python("3.12")
def recent_events(
    events: Annotated[
        pa.Table,
        Model(
            "raw.events",
            projection_schema=EventColumns,
            filter="event_date >= '2025-01-01'",
        ),
    ],
) -> Annotated[pa.Table, RecentEvents]:
    """Events of the reporting window."""
    return events
```

`filter=` is a string literal: no f-strings, no variables. It works here because `raw.events` is a catalog table. The same `filter=` on `Model("recent_events")` in a child model of the same project is rejected by the planner; filter inside the child's body instead, or materialize `recent_events` and read it from a later project. Use a parameter for values that change per run: `filter="event_date >= $start_date"`, with `start_date` declared under `parameters` in `bauplan_project.yml`. Keep the filtered column bare: `LOWER(event_type) = 'click'` cannot prune files.

## 4. One shared preparation

Before: three models each load `raw.fx_rates` whole and derive monthly rates their own way.

After: one narrow model that all three depend on.

```python
class RateColumns(TableSchema):
    """The rate columns needed to derive monthly rates."""

    currency: String | None
    rate_date: Date32 | None
    rate: Float64 | None


class MonthlyFxRates(TableSchema):
    """Last available rate per currency and month."""

    currency: String | None
    month: Date32 | None
    rate: Float64 | None


@bauplan.model()
@bauplan.python("3.12", pip={"polars": "1.42.1"})
def monthly_fx_rates(
    rates: Annotated[pa.Table, Model("raw.fx_rates", projection_schema=RateColumns)],
) -> Annotated[pa.Table, MonthlyFxRates]:
    """Last rate per currency and month."""
    import polars as pl

    return (
        pl.DataFrame(rates)
        .sort("rate_date")
        .group_by("currency", pl.col("rate_date").dt.month_start().alias("month"))
        .agg(pl.col("rate").last())
        .to_arrow()
    )
```

Consumers declare `fx: Annotated[pa.Table, Model("monthly_fx_rates")]`.

## 5. Parent computes, children derive

```python
@bauplan.model()
@bauplan.python("3.12", pip={"polars": "1.42.1"})
def revenue_base(
    lines: Annotated[pa.Table, Model("billing_lines")],
    fx: Annotated[pa.Table, Model("monthly_fx_rates")],
) -> Annotated[pa.Table, RevenueBase]:
    """Revenue lines in the reporting currency, shared by every revenue output."""
    ...


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.12", pip={"polars": "1.42.1"})
def recurring_revenue(
    base: Annotated[pa.Table, Model("revenue_base")],
) -> Annotated[pa.Table, RecurringRevenue]:
    """Recurring revenue per account and month."""
    ...


@bauplan.model(materialization_strategy="REPLACE")
@bauplan.python("3.12", pip={"polars": "1.42.1"})
def one_off_revenue(
    base: Annotated[pa.Table, Model("revenue_base")],
) -> Annotated[pa.Table, OneOffRevenue]:
    """One-off revenue per transaction."""
    ...
```

`RevenueBase`, `RecurringRevenue` and `OneOffRevenue` are `TableSchema` classes declared as in pattern 1. The expensive join and conversion run once instead of once per output.

## 6. Merge a whole-table handoff

Before: project A materializes `stage_one`; project B reads `stage_one` whole, with no `filter=`, and nothing else reads it. Two jobs, one write, one read.

After: B's models move into A, and `stage_one` becomes a non-materialized model. If B needs a filtered slice of `stage_one`, the split must stay, because `filter=` only applies to catalog inputs.

## 7. Partition for read pruning

```python
class EventDateColumns(TableSchema):
    """The event columns kept after cleaning."""

    event_id: String | None
    account_id: String | None
    event_date: Date32 | None


class CleanEvents(TableSchema):
    """Cleaned events, partitioned by month because every consumer reads one month at a time."""

    event_id: String | None
    account_id: String | None
    event_date: Date32 | None


@bauplan.model(materialization_strategy="REPLACE", partitioned_by=["month(event_date)"])
@bauplan.python("3.12")
def events_clean(
    events: Annotated[pa.Table, Model("raw.events", projection_schema=EventDateColumns)],
) -> Annotated[pa.Table, CleanEvents]:
    """Cleaned events, partitioned by month because every consumer reads one month at a time."""
    return events
```

Consumers in later projects read `Model("analytics.events_clean", projection_schema=..., filter="event_date >= '2026-08-01' AND event_date < '2026-09-01'")` and touch only that month's files. Accepted transforms are `year`, `month`, `day` and `hour`, plus plain columns; there is no `bucket()` or `truncate()`.

## 8. Incremental output

```python
class LineColumns(TableSchema):
    """The line columns needed for monthly totals."""

    account_id: String | None
    posted_at: TimestampMicro | None
    amount: Float64 | None


class MonthlyTotals(TableSchema):
    """Billed amount per account and month, rewritten only for the open months."""

    month: Date32
    account_id: String | None
    amount: Float64 | None


@bauplan.model(
    materialization_strategy="OVERWRITE_PARTITIONS",
    partitioned_by=["month"],
    overwrite_filter="month >= $first_open_month",
)
@bauplan.python("3.12", pip={"polars": "1.42.1"})
def monthly_totals(
    lines: Annotated[
        pa.Table,
        Model("raw.lines", projection_schema=LineColumns, filter="posted_at >= $first_open_month"),
    ],
) -> Annotated[pa.Table, MonthlyTotals]:
    """Recompute only the open months and leave closed months untouched."""
    ...
```

`first_open_month` is a parameter declared in `bauplan_project.yml` and passed on each run as the first day of a month (`YYYY-MM-01`); it is used only inside the filter strings, so the function does not need a `Parameter` argument. Every row the model returns must fall inside `overwrite_filter`, and every row the filter deletes must be recomputed: the runtime appends all returned rows without checking them. `month` is an explicit date column because the output is aggregated per month; for row-level outputs, a transform partition such as `month(posted_at)` with `overwrite_filter` on `posted_at` works too. Use only when earlier periods cannot change.
