---
name: bauplan-dag-optimization
description: "Optimizes an existing, poorly structured Bauplan DAG so it runs faster without changing its outputs. Reads the code and profiles the data, then restructures the models, decides what is materialized, narrows inputs with projections and filters, merges or splits projects, and proposes a data-driven partitioning plan with concrete columns and transforms. Use whenever the user has a working pipeline and wants it faster or leaner on Bauplan, for example 'optimize this DAG', 'this pipeline is slow, speed it up', 'we write too many intermediate tables', 'should I partition this table', 'make this pipeline more efficient on Bauplan'. Do NOT use to scaffold a new pipeline from scratch (use bauplan-data-pipeline) or to diagnose a single failed job (use bauplan-debug-and-fix-pipeline)."
allowed-tools:
  - Bash(bauplan:*)
  - Bash(uv:*)
  - Bash(uvx:*)
  - Read
  - Write
  - Edit
  - Glob
  - Grep
  - WebFetch(domain:docs.bauplanlabs.com)
---

# Optimize a Bauplan DAG

## Contents

[Mental model](#mental-model) · [Workflow](#workflow) · [1. Inventory](#1-inventory-the-dag) · [2. Profile](#2-profile-the-data) · [3. Materialization](#3-materialize-only-what-is-consumed) · [4. Inputs](#4-narrow-every-input) · [5. Model structure](#5-restructure-the-models) · [6. Project boundaries](#6-merge-and-split-projects) · [7. Partitioning](#7-propose-partitioning) · [8. Verify and measure](#8-verify-and-measure) · [Proposal](#proposal)

References: [partitioning.md](partitioning.md) for the profiling queries and partitioning heuristics, [patterns.md](patterns.md) for before/after code.

This skill uses the typed SDK (0.3.0+) syntax: inputs are `Annotated[pa.Table, bauplan.Model(...)]`, projections are `TableSchema` classes passed as `projection_schema=`, and every model declares its output schema in its return annotation. If the project is on an older SDK, migrate it first with the `bauplan-migrate-to-typed-sdk` skill, or ask the user before optimizing old-style code.

## Mental model

Bauplan handles I/O, Arrow conversion between models, where intermediate data lives and when work is scheduled. A model that is not materialized is still handed to its children by the platform; you do not manage memory or placement, and you should not try to. Performance comes from data modeling:

1. How much data each model reads: columns and rows.
2. How many tables are written to the lake and read back.
3. How the work is split into models and into projects.
4. How large tables are partitioned, so that reads prune and runs rewrite only what changed.

The biggest wins are almost always I/O: fewer materialized intermediates, narrower inputs, and partitioned tables that consumers can read slice by slice.

### Platform facts that drive the decisions

1. **Filters apply only to catalog inputs.** `filter=` on `bauplan.Model(...)` works only on a table that already exists in the catalog when the run starts. Setting it on a model produced in the same run is rejected by the planner ("Apply the filter inside the function"). To read a slice of an intermediate, materialize it in one project and read it with `filter=` from a later project run.
2. **Catalog filters and projections are pushed into the scan.** The filter drives Iceberg file pruning (partition values and file statistics) before any data reaches the model, and only the projected columns are read. Pruning needs plain comparisons on columns: `event_date >= '2025-01-01'` prunes, `LOWER(region) = 'emea'` or other functions on the column do not. Parameters can be used inside filters with `$name` syntax, for example `filter="event_date >= $start_date"`. The planner warns about unfiltered scans of tables of 20 GB or more.
3. **Non-materialized intermediates are cheap.** A model without a `materialization_strategy` (the default is `NONE`) does not write to object storage and no Iceberg commit happens. A materialized output is additionally written to object storage and committed to the catalog (`REPLACE` drops and rewrites the whole table).
4. **Light models run in parallel.** Within a job, when possible, independent models run in parallel. As a rule of thumb, the lighter the jobs, the easier it is to parallelize them. Heavier jobs, which take up a lot of memory, can result in an overall job slowdown.
5. **Declarations are read statically.** `filter=`, `projection_schema=`, the schema classes and the decorators are parsed from the source before execution, so filters must be string literals (a `$name` parameter inside the literal is fine), not f-strings or variables.

## Workflow

Do steps 1 and 2 before proposing anything. Do not ask the user what to optimize; find it.

### 1. Inventory the DAG

Read every `bauplan_project.yml`, `models.py` and SQL model. Build one table with a row per model:

| Column | What to record |
| --- | --- |
| model | function name and project |
| inputs | each `bauplan.Model(...)` with its `projection_schema=` and `filter=` |
| columns used | the columns the body actually touches |
| materialized | `materialization_strategy` and `partitioned_by`, if set |
| consumers | models in the same project, other projects, and anything outside the DAG |
| operations | joins, `group_by` keys, window partition keys, sorts, date filters inside the body |

Ask the user only one question if the code cannot answer it: which tables are read by something outside the DAG (dashboards, other teams, agents, a semantic layer).

### 2. Profile the data

Run read-only queries on a pinned ref to measure what the code alone cannot tell you. Use the procedures in [partitioning.md](partitioning.md):

1. `records` and `size` of every catalog input and every materialized output, from `client.get_table(...)`, plus its current partitioning.
2. For every column used in a filter: distinct count, null share, and the rows of the largest value (skew).
3. For every date or timestamp column used in a filter: range and rows per month or per day.

Keep the numbers; the proposal cites them.

### 3. Materialize only what is consumed

Walk backwards from consumption. A model gets a `materialization_strategy` only if:

1. something outside this project reads it;
2. it is the final output;
3. a later project needs to read a filtered slice of it (see the `filter=` rule above);
4. the user wants it as an inspection checkpoint.

Everything else stays non-materialized. Each removed materialization saves one write to object storage and one Iceberg commit. Instead of rebuilding the whole table with `REPLACE` on every run, it is preferable to use, when applicable, an incremental approach using either `OVERWRITE_PARTITIONS` or `APPEND`.

If the original DAG materialized tables that would no longer have a materialization strategy, ask the user how to proceed: a) keep materializing them as before, b) keep the existing tables but stop materializing them, or c) drop the existing tables and stop materializing them.

### 4. Narrow every input

1. Project every catalog input to the columns the body uses, with a `projection_schema=` class that lists only those columns. A missing projection reads the whole table. Take the column types from `bauplan table get <namespace>.<table>` and mark nullable columns `| None`.
2. Move row filters onto catalog inputs with `filter=` when the rows would be discarded anyway. Do not move a filter across a join or an aggregation if that changes multiplicity or totals. For an input that is a model of the same run, filter inside the consuming function instead.
3. Write filters as plain comparisons on columns, so they can prune files. Replace run-dependent literals with `$name` parameters rather than editing the code per run.
4. Pruning is strongest on partition columns; file statistics help only when the data is clustered on the filtered column. Use this when choosing partition columns (step 7).
5. Drop columns from intermediate frames as soon as no later model needs them, and remove them from the model's output schema too: the declared output schema is exhaustive, so a narrower model needs a narrower schema.

### 5. Restructure the models

1. Split a function that does the entire pipeline into one model per table transformation: preparation of each source, each join, each change of grain, each output. The function signatures should show the data flow. Every new model needs its own output `TableSchema` in its return annotation, derived from what the transformation actually produces, including casts where Polars yields a type the schema does not allow (for example unsigned integers from `pl.len()` or a hash).
2. Merge models that only rename or cast for a single consumer into that consumer.
3. When several models load and clean the same input, create one preparation model and make them depend on it.
4. When several outputs share an expensive join or aggregation, compute it once in a parent model and derive each output in a child.
5. Keep each model's working set small enough. When one model joins or aggregates far more data than the others, it is the first candidate to narrow or split into steps.
6. Prefer Polars inside models and Arrow at the boundaries; avoid round trips to pandas, which copies the data, inside a chain.

### 6. Merge and split projects

Merge two projects when the second reads the first's output whole, without a filter, and nothing else reads that output: the intermediate becomes a non-materialized model, saving a write, a read and a job submission.

Split a project when partitioning (step 7) requires consumers to read slices of an intermediate or when it holds independent pipelines that could run as separate jobs. Independent projects can then run in parallel; dependent ones run in order.

### 7. Propose partitioning

Do not leave this to the user. From the inventory and the profile, make an explicit recommendation for every table above about 1 GB and every materialized output, using the decision procedure in [partitioning.md](partitioning.md). There are two kinds of partitioning, and a table may need more than one:

| Kind | Goal | Column choice |
| --- | --- | --- |
| Read pruning | Consumers read fewer files | The column consumers filter on, usually a date with a `year`, `month`, `day` or `hour` transform, or a low-cardinality category |
| Incremental writes | Rewrite only the periods that changed | The time column that defines which rows change between runs |

For each recommendation, state the table, the kind, the column and transform, the expected number of partitions and rows or GB per partition, the evidence (numbers from step 2 and the code lines that filter or group on the column), what changes in the DAG (new `partitioned_by`, new `filter=`, a project split), and the risk. Also state explicitly which large tables should not be partitioned and why. The user makes the final call, but always give them a concrete default.

### 8. Verify and measure

An optimization must not change results. For every output, compare the optimized run with the original run on the same inputs: same schema and dtypes, same rows including duplicates, same NULL positions. Restructuring can change row order and the order of float summation; if a result depends on either (keep-first deduplication, cumulative sums, float totals), make the order explicit or report the difference.

Measure on the same branch state with the same settings, and report per-job wall time with job ids for both versions. Run both with cache disabled (`--no-cache` in the CLI, `cache=False` in the SDK), because the platform skips tasks whose outputs it already holds, which makes a rerun look faster than it is.

Besides data, Bauplan stores on Iceberg, at every run, the table description (the model function's docstring, or the output `TableSchema` class docstring if the function has none), as well as column documentation (in the form of `TableField(doc="...")`). Hence, do ensure that when rewriting a DAG these metadata are kept consistent, too.

## Proposal

Before editing code, present:

1. The inventory table, with the problems marked: unprojected inputs, unnecessary materializations, duplicated preparation, whole-table handoffs between projects.
2. The partitioning recommendations from step 7, each with its evidence.
3. The new DAG: models per project, which are materialized and why, and the order projects run in.
4. The expected effect in plain numbers: tables no longer written, GB no longer read, jobs that become parallel.

Implement after the user approves, one project at a time, verifying each before the next: `uvx ruff check` and `uvx ty check` on the project, then `bauplan run --dry-run`, then a full run on an isolated data branch (`<username>.<branch>`, never `main`). The dry run executes every model, so output contract mismatches already surface there, as `ModelOutputContractError`.
