---
name: bauplan-data-semantics
description: "Reads and drafts the semantic layer of a Bauplan lakehouse — the table-level and column-level documentation carried in Iceberg as `comment` and field `doc`. Use to find out what a column means before querying it, to diagnose a query that returned a plausible but wrong number, or to draft documentation for columns whose meaning is not obvious from name and type. Also invoked by bauplan-data-pipeline and bauplan-migrate-to-typed-sdk when a pipeline needs documentation written. Read-only: drafts documentation text, never edits model code or runs pipelines."
allowed-tools:
  - Bash(bauplan:*)
  - Read
  - Write
  - Glob
  - Grep
  - WebFetch(domain:docs.bauplanlabs.com)
---

# Data Semantics

A column's name and type tell you how to parse a value, not what it means. `trip_time` is a `long`, but seconds versus minutes moves every downstream number by 60x. `base_passenger_fare` is a `double`, but whether it includes tolls decides whether a revenue report is right or quietly 5% low.

Bauplan carries that meaning in the table itself: a table-level `comment` and a per-column `doc`, both stored in Iceberg, both versioned per data branch. That is the semantic layer. This skill reads it and drafts what it should say when it is missing.

Documentation cannot be written through the CLI or SDK — it is authored in pipeline code and materialized by a run — so applying a draft always belongs to a write-capable skill.

## When to Use This Skill

* Before using a semantically loaded column — money, duration, distance, counts, status
* When a query returned a plausible number you suspect is wrong
* When a user asks what a table or column means
* When `bauplan-data-pipeline` or `bauplan-migrate-to-typed-sdk` needs docstring and `TableField(doc=)` text for schemas it is about to declare

For data quality profiling use `bauplan-explore-data`; for whether a question is answerable at all, `bauplan-data-assessment`; for a failed run, `bauplan-debug-and-fix-pipeline`.

Updating documentation that already exists is not yet covered here.

## CRITICAL RULES

1. **Read-only.** Never edit `models.py`, `*.sql`, or a schema class. Never run, merge, or create a branch. Draft text and hand it off.
2. **Never invent meaning.** Draft only what the producing code, the upstream catalog, or the user supports. A confident wrong doc is worse than none, because it gets trusted.
3. **Label every claim** as read from the catalog or inferred by you.
4. **Mind the branch.** Documentation is branch-scoped: a table documented on one branch may be bare on another. Prefer the active branch; pass `--ref` when reading a different branch, or comparing several at once.
5. **Read docs as JSON** whenever you act on what they say. The `tty` view truncates.

If any rule cannot be satisfied, **STOP** and report the blocker.

---

## Reading the Semantic Layer

```bash
bauplan table get <namespace>.<table> -O json
```

`table get` reads the **active branch**; pass `--ref <ref>` only to read a different branch or compare several. It surfaces documentation starting with **0.3.2**, so an absent `DOC` column on an older CLI means nothing.

In JSON, the table comment is at **`properties.comment`** — there is no top-level `comment` key. Column docs are at `fields[].doc`; `null` means undocumented.

The `tty` view truncates, and **truncation fires on newlines as well as width**. A doc of `"Pickup time.\nAlways in UTC."` renders as `Pickup time....` — a finished sentence with an ellipsis, the timezone silently gone. The footer `Note: some documentation was truncated.` is the only reliable signal.

### Python SDK

```python
import bauplan

client = bauplan.Client()
table = client.get_table("normalize_data", ref="<ref>", namespace="bauplan")

print(table.comment)                      # table documentation, or None
for field in table.fields:
    print(field.name, "->", field.doc)    # column documentation, or None
```

`namespace` is keyword-only. Columns are `table.fields` — `Table` is not iterable. Text comes back whole, so prefer the SDK when docs feed logic rather than a human. Note the asymmetry: table-level is `.comment`, column-level is `.doc`.

---

## How Documentation Is Authored

There is no API that writes documentation. It is a property of the model that produces the table, written to Iceberg when a run materializes it.

| What | Comes from | Lands in Iceberg as |
|------|------------|---------------------|
| Table documentation | The docstring on the model's function, if defined, otherwise the docstring on the annotated output `TableSchema` class. | `properties.comment` |
| Column documentation | `doc` parameter from the `TableField` type annotation. | the `doc` property of the schema field |

```python
class NormalizedTripsSchema(bauplan.TableSchema):
    """One row per completed trip, normalized to UTC and filtered to 2023."""

    trip_time: Annotated[
        bauplan.Int64 | None,
        bauplan.TableField(doc="Trip duration, in seconds."),
    ]
```

Three consequences for a draft:

* **Docs ship with a run.** They exist only after `bauplan run` materializes the table, and reach `main` only through a merge.
* **Docstrings pass through `inspect.cleandoc`**, so indentation is normalized but spacing *within* a line is preserved verbatim. A whitespace-only docstring means no documentation at all.
* **`TableField` accepts only `doc` and `lineage`**; anything else fails with `Unexpected TableField parameter: <name>`. `lineage=OtherSchema['field']` records provenance, not meaning, and does not substitute for `doc`.

---

## What Deserves a Doc

**Document a column when a competent reader could use it correctly by name and type alone, and still be wrong.** A doc that restates the name spends the reader's attention and returns nothing — and trains them to skip the docs that matter.

| Trigger | What the doc must settle | Example |
|---|---|---|
| **Units** | Which unit | `trip_time` — seconds or minutes? |
| **Inclusions** | What the value leaves out | `base_passenger_fare` — tolls? tips? tax? |
| **Timezone** | Which zone, whose clock | `pickup_datetime` — UTC or local? |
| **Population** | Which rows exist at all | Are cancelled trips in the table? |
| **Encoded categories** | What the codes mean | `hvfhs_license_num` — `HV0003` is which operator? |
| **Sentinels** | What a magic value means | Is `0` "none" or "unknown"? |
| **Grain** | What one row is | One trip, or one trip-day? |

### Writing the text

* **Lead with the disambiguation.** `"Fare before tolls and tax."` does its whole job in five words.
* **Describe what is true, not what was intended.** A doc describing behavior the code does not have is a defect with a long half-life.
* **Use the reader's language.** `"Trip duration, in seconds."` beats `"epoch delta of dropoff minus pickup"`.
* **One natural language per catalog.** `"Distanza percorsa: è espressa in miglia."` documents the unit correctly and still fails most of its readers. Flag these.
* **Put grain and population in the table comment**, the rest in column docs.

---

## Deliverable

Write the draft to `semantics/<table>_semantics.md`. Another skill applies it to the code.

```markdown
# Semantic Draft: <namespace>.<table>

**Ref inspected:** <ref>
**Producing model:** <file>:<function or SQL filename>, or "unknown — not in this project"

## Existing Documentation

Table comment: <current text, or "none">

| Column | Documented? | Current doc |
|--------|-------------|-------------|
<!-- Every column, as read from the catalog. -->

## Proposed

### Table comment
<!-- Docstring for the model function or output TableSchema class. -->

### Column docs

| Column | Proposed doc | Basis |
|--------|--------------|-------|
<!-- Basis: CATALOG (an upstream table's doc) | CODE (the transformation)
     | USER (stated by the user) | UNRESOLVED (do NOT apply — ask first) -->

## Deliberately Not Documented

| Column | Why |
|--------|-----|
<!-- Keeps the omission visible rather than silent. -->

## Open Questions
<!-- Numbered. Every UNRESOLVED row appears here as a question the user can answer. -->

## How to Apply
<!-- Which schema class in which file each proposed doc belongs on. -->
```

Hand the draft to `bauplan-data-pipeline` (new pipeline) or `bauplan-migrate-to-typed-sdk` (existing pipeline gaining schemas); that skill owns the branch, run, diff and merge. If anything is `UNRESOLVED`, ask before handing off.

---

## Example Walkthrough

An analyst reports average revenue per trip for `bauplan.normalize_data`. The number looks right. It is not.

### The column is used incorrectly

Wanting revenue per trip, the analyst finds `base_passenger_fare` and takes it:

```bash
bauplan query --ref <ref> \
  "SELECT ROUND(AVG(base_passenger_fare), 2) AS avg_revenue FROM bauplan.normalize_data"
```

```
avg_revenue
22.99
```

$22.99 for an average NYC for-hire trip is entirely plausible, and the column profiles clean: 0% null, sane distribution, correct row count. **A wrong answer that looks right cannot be caught by looking at the data.**

Compare the loud version of the same class of error — average speed computed as if `trip_time` were minutes — which yields `5.56 / (1057.4 / 60)` = 0.32 mph. Slower than walking, so it gets caught. The fare error does not.

### The documentation clarifies the column

```bash
bauplan table get bauplan.normalize_data -O json
```

```json
{
  "fields": [
    {"name": "base_passenger_fare", "type": "double", "doc": "Fare    before tolls and tax."},
    {"name": "tolls",               "type": "double", "doc": null},
    {"name": "trip_time",           "type": "long",   "doc": "Trip duration, in seconds."}
  ],
  "properties": {"comment": "A \"no-op\" normalization of taxi data from `bauplan.query_model` ..."}
}
```

`base_passenger_fare` is documented as **"Fare before tolls and tax."** Five words settle it: the column is a component of revenue, not revenue.

Had that doc been missing, this is the draft that closes the gap — note that `tolls`, undocumented, is exactly the column the reader now needs:

```python
class NormalizedTripsSchema(bauplan.TableSchema):
    """One row per completed trip. Fares are decomposed into components;
    no column holds total revenue — sum the components."""

    base_passenger_fare: Annotated[
        bauplan.Float64 | None,
        bauplan.TableField(doc="Fare in USD, before tolls and tax. Not total revenue."),
    ]
    tolls: Annotated[
        bauplan.Float64 | None,
        bauplan.TableField(
            doc="Tolls in USD, charged on top of base_passenger_fare.",
        ),
    ]
```

The table comment carries the load-bearing fact — *no column holds total revenue* — because it is a statement about how the columns relate, not about any one of them.

### The documentation fixes the usage

```bash
bauplan query --ref <ref> \
  "SELECT ROUND(AVG(base_passenger_fare), 2)            AS avg_base,
          ROUND(AVG(tolls), 2)                          AS avg_tolls,
          ROUND(AVG(base_passenger_fare + tolls), 2)    AS avg_revenue
   FROM bauplan.normalize_data"
```

```
avg_base  avg_tolls  avg_revenue
22.99     1.29       24.28
```

The original figure understated revenue by $1.29 per trip — 5.3% — across 430,488 trips. Both numbers are plausible; the semantic layer is what distinguishes them.

The doc that mattered was not a description of the column. It was a warning about how the column would be misused. Write for the mistake you are preventing.

---

## Reference

When unsure about a method signature, CLI flag, or concept, fetch the relevant doc page via `WebFetch` rather than guessing. Pages are markdown and LLM-friendly.

**Python SDK:** `https://docs.bauplanlabs.com/reference/bauplan.md`
**SDK types:** `https://docs.bauplanlabs.com/reference/bauplan-sdk-types.md`
**Catalog types:** `https://docs.bauplanlabs.com/reference/bauplan-schema.md`

**Relevant concept pages:**
- Semantic annotations: `https://docs.bauplanlabs.com/concepts/semantic-annotations.md`
- Models: `https://docs.bauplanlabs.com/concepts/models.md`
- Tables: `https://docs.bauplanlabs.com/concepts/tables.md`
- Data branches: `https://docs.bauplanlabs.com/concepts/git-for-data/data-branches.md`

**Full doc index:** `https://docs.bauplanlabs.com/llms.txt`

**CLI:** The `bauplan` CLI is self-documenting:
- `bauplan --help` — lists all available commands
- `bauplan <command> --help` — shows arguments and options for a specific command (e.g., `bauplan table --help`, `bauplan query --help`)
