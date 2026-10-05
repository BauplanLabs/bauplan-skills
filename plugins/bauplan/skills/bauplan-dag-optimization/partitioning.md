# Partitioning: profile, decide, propose

## Contents

[Profile](#profile) · [Decide](#decide) · [Read pruning](#read-pruning) · [Incremental writes](#incremental-writes) · [When not to partition](#when-not-to-partition) · [Proposal format](#proposal-format)

The model makes the first decision from data and code, and the user confirms it. Every threshold below is a starting heuristic; when the measured data says otherwise, follow the data and say why.

## Profile

Run everything read-only, on a pinned ref (a commit or tag, not a moving branch head). Use the `bauplan-explore-data` skill if it is available.

Table size and current layout:

```python
import bauplan

client = bauplan.Client()
table = client.get_table("namespace.table_name", ref="branch@commit")
table.records, table.size, [(field.name, field.transform) for field in table.partitions]
```

Cardinality, nulls and skew of a candidate column:

```python
client.query(
    """
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT region) AS distinct_values,
        SUM(CASE WHEN region IS NULL THEN 1 ELSE 0 END) AS null_rows
    FROM namespace.table_name
    """,
    ref="branch@commit",
)

client.query(
    """
    SELECT region, COUNT(*) AS rows_for_value
    FROM namespace.table_name
    GROUP BY region
    ORDER BY rows_for_value DESC
    LIMIT 20
    """,
    ref="branch@commit",
)
```

Distribution over time of a date column:

```python
client.query(
    """
    SELECT DATE_TRUNC('month', event_date) AS month, COUNT(*) AS rows_in_month
    FROM namespace.table_name
    GROUP BY 1
    ORDER BY 1
    """,
    ref="branch@commit",
)
```

Bytes per row is `size / records`; multiply it by a group's row count to estimate the GB of a partition. Profile every column that the code filters on, and nothing else.

**Note well**: it is recommended to start off from a recent slice of the data and profile that one; move to full-table scans if and only if it is strictly needed.

## Decide

For every table larger than about 1 GB and every materialized output, go through the two kinds below in order and record a verdict for each: recommend, reject with reason, or not applicable.

## Read pruning

Goal: consumers that filter the table read only the matching files.

Recommend when all of these hold:

1. Some consumer filters the table on the column, with `filter=` on the input or a filter inside the model body that could move to `filter=`.
2. The filter typically selects a small share of the table, for example one month out of many.
3. The resulting partitions are not tiny: aim for at least a few hundred MB per partition and fewer than about a thousand partitions in total.

Choosing the transform for a date or timestamp column: use `day` when one day holds roughly a GB or more, `month` when a month holds between a few hundred MB and tens of GB, `year` below that. For a categorical column, use the raw value when it has fewer than about a hundred distinct values and no single value holds most of the rows.

Wire it: set `partitioned_by` on the producing model, for example `partitioned_by=["month(event_date)", "region"]`, and make sure consumers filter on that column with a plain comparison in `filter=`. Accepted entries are a plain column (identity) and `year(col)`, `month(col)`, `day(col)`, `hour(col)`; `bucket()`, `truncate()` and nested functions are rejected by the planner. Remember that `filter=` only applies to catalog inputs, so the producer and the filtering consumer must be in different project runs. Tables of 20 GB or more that consumers scan without a filter trigger a planner warning: treat each one as a read-pruning candidate.

## Incremental writes

Goal: a run rewrites only the periods whose data changed instead of the whole table.

Recommend when the output is large, organized by a time column, and the inputs only add or change recent periods. Materialize the output with `OVERWRITE_PARTITIONS`, which deletes the rows matching `overwrite_filter` and appends the new ones, and filter the inputs to the same periods. The planner enforces:

1. `partitioned_by` is set;
2. every column in `overwrite_filter` is a partition column;
3. `overwrite_filter` uses only `=`, `!=`, `<`, `>`, `<=`, `>=`, `AND`, `OR`, `IN`, `NOT IN` on columns and literals, with no function calls and no `BETWEEN`.

## When not to partition

Reject partitioning, and say so in the proposal, when:

1. The table is under about 1 GB.
2. No consumer filters or groups on the candidate column.
3. The column is a near-unique identifier used as an identity partition (millions of tiny partitions).
4. One value holds most of the rows.

## Proposal format

Give one row per table, including the rejected ones:

| Table | Records / size | Kind | Column and transform | Partitions, size per partition | Evidence | DAG changes | Risk |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `sales.events` | 2.1 B rows / 180 GB | Read pruning | `month(event_date)` | 36, about 5 GB | 4 consumers filter on `event_date`, 3 of them on one month | `partitioned_by` on producer; `filter=` on 4 inputs | none known |
| `ref.accounts` | 4 M rows / 0.3 GB | none | | | small | | |

End with a default: "Recommended: apply row 1; leave ref.accounts unpartitioned." The user decides, but never without a concrete recommendation.
