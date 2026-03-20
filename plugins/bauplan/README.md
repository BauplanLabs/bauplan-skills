# Bauplan Skills Plugin

Skills for working with [Bauplan](https://www.bauplanlabs.com/) data lakehouses via Claude Code.

## What is Bauplan?

Bauplan is a serverless data lakehouse platform. It lets you build data pipelines as Python/SQL DAGs, run them on managed infrastructure, and manage data with Git-like branching on Apache Iceberg tables. All data operations are branch-isolated and merge-safe.

## Installation

1. Add the marketplace:
```
/plugin marketplace add https://github.com/BauplanLabs/bauplan-skills
```

2. Install the plugin:
```
/plugin
```
Select **Browse and install plugins** → **bauplan-skills** → press `Space` to select **bauplan** → press `i` to install.

3. Restart Claude Code.

## Skills

### bauplan-explore-data

Read-only exploration of lakehouse tables. Inspects namespaces, schemas, samples, and runs profiling queries. Produces a `summary.md` with findings.

**When to use:** You want to understand what data exists before building anything.

### bauplan-data-assessment

Assesses whether a business question can be answered with available data. Maps business concepts to tables and columns, checks quality, validates semantic fit, and renders a verdict (answerable / partially answerable / not answerable).

**When to use:** A user asks "can we figure out X from what we have?"

### bauplan-data-pipeline

Creates data pipeline projects with Python and SQL models. Handles project setup (`bauplan_project.yml`), model writing with I/O pushdown, materialization strategies, and dry-run validation.

**When to use:** Starting a new pipeline or adding models to an existing one.

### bauplan-safe-ingestion

Ingests data from S3 using the Write-Audit-Publish (WAP) pattern: import on an isolated branch, validate, then merge to main. Supports three validation paths depending on how well the user knows the data.

**When to use:** Loading new data from S3 into the lakehouse.

### bauplan-debug-and-fix-pipeline

Structured diagnosis and repair for failed pipeline jobs. Pins the exact failing data state, collects evidence, traces root cause upstream, applies a minimal fix, and reruns. Evidence first, changes second.

**When to use:** A `bauplan run` has failed and you need to understand why.

### bauplan-data-quality-checks

Generates data quality check code in two forms: pipeline expectations (`expectations.py` with `@bauplan.expectation()`) or ingestion validation (`validate_import()` for WAP scripts). Derives checks from pipeline code or user specifications.

**When to use:** Adding quality gates to a pipeline or ingestion workflow.

## How It Works

Each skill is a structured workflow that Claude follows autonomously. Skills use the Bauplan Python SDK and CLI for all data operations. Key principles:

- **Branch safety** — never operate on `main` directly
- **Explicit refs** — all queries target a specific branch or commit
- **Phased execution** — skills run in sequential phases with user checkpoints
- **Evidence first** — understand the data before making changes

## Plugin Components

### Skills

| Skill | Folder |
|-------|--------|
| bauplan-explore-data | `skills/bauplan-explore-data/` |
| bauplan-data-assessment | `skills/bauplan-data-assessment/` |
| bauplan-data-pipeline | `skills/bauplan-data-pipeline/` |
| bauplan-safe-ingestion | `skills/bauplan-safe-ingestion/` |
| bauplan-debug-and-fix-pipeline | `skills/bauplan-debug-and-fix-pipeline/` |
| bauplan-data-quality-checks | `skills/bauplan-data-quality-checks/` |

### Files

```
plugins/bauplan/
  .claude-plugin/
    plugin.json              # Plugin metadata
  skills/
    bauplan-explore-data/
      SKILL.md               # Data exploration skill
    bauplan-data-assessment/
      SKILL.md               # Data feasibility assessment
    bauplan-data-pipeline/
      SKILL.md               # Pipeline creation skill
      examples.md            # Advanced pipeline examples
    bauplan-safe-ingestion/
      SKILL.md               # S3 ingestion with WAP pattern
    bauplan-debug-and-fix-pipeline/
      SKILL.md               # Pipeline debugging skill
    bauplan-data-quality-checks/
      SKILL.md               # Quality check generation
      ingestion_validation.py    # Example: WAP validation
      pipeline-expectations.py   # Example: pipeline expectations
  README.md
```

## Metadata

- **Version:** 1.0.0
- **Author:** [Bauplan Labs](https://www.bauplanlabs.com/)
- **License:** MIT
