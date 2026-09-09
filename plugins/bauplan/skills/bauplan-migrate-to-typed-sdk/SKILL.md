---
name: bauplan-migrate-to-typed-sdk
description: "Migrates an existing bauplan pipeline project from the old SDK style (0.1.x or 0.2.x) to the typed SDK (0.3.+). Use when asked to migrate a pipeline to the new SDK, add type annotations to a bauplan project, upgrade to bauplan 0.3, or adopt TableSchema / the semantic layer."
allowed-tools:
  - Bash(bauplan:*)
  - Bash(uv:*)
  - Bash(pip:*)
  - Bash(ruff:*)
  - Bash(ty:*)
  - Read
  - Write
  - Edit
  - Glob
  - Grep
  - WebFetch(domain:docs.bauplanlabs.com)
---

# Migrating a Bauplan Pipeline to the Typed SDK

Bauplan 0.3.x replaces the old declaration style with type annotations that double as a semantic layer: column projections and output contracts become `TableSchema` classes, model inputs become `Annotated[pa.Table, Model(...)]` parameters, and run parameters become `Annotated[<type>, Parameter(...)]`. This skill takes an existing old-style project, rewrites it to the typed style, and validates the result until the pipeline runs green.

The complete old-to-new pattern catalog with real code is in [examples.md](examples.md). Read it before rewriting any file.

## What changes, in one look

| Old (0.1.x or 0.2.x) | New (0.3.x) |
|---|---|
| `data=bauplan.Model("t", columns=[...], filter=...)` | `data: Annotated[pa.Table, Model("t", projection_schema=SomeSchema, filter=...)]` |
| `@bauplan.model(columns=[...])` | `@bauplan.model()` + `-> Annotated[pa.Table, OutputSchema]` |
| no schema objects | `class SomeSchema(TableSchema)` with typed fields, optional `TableField(doc=, lineage=)` |
| `x=bauplan.Parameter("x")` | `x: Annotated[float, Parameter("x")]` |
| return list/pandas/polars/arrow | return an actual `pa.Table` |

## CRITICAL: Branch Safety

> **NEVER run pipelines on `main` branch.** Always checkout to a test branch first: `bauplan checkout -b <username>.<branch_name> --from-ref main`.
> ALWAYS validate the migrated pipeline on a separate data branch.

Get the username by running `bauplan info`.

## Input

Before starting, you need to find all the Bauplan projects that need migration. A Bauplan project is a folder containing `bauplan_project.yml` or `bauplan_project.yaml`.

You can find them using e.g. `find . -name 'bauplan_project.yml' -o -name 'bauplan_project.yaml'`.

## Migration Workflow

### Step 1: Inventory

Read every `models.py`, `expectations.py`, other `*.py` model files, `*.sql` files, and `bauplan_project.yml` (also accepted as `bauplan_project.yaml`) in the project. List:

- every `@bauplan.model` and `@bauplan.expectation` function
- every `bauplan.Model(...)` input and its `columns` / `filter` / other kwargs
- every `bauplan.Parameter(...)` and its `type:` in `bauplan_project.yml`
- every `columns=[...]` on a `@bauplan.model()` decorator
- every model return statement and what it returns (arrow, polars, pandas, list of dicts)

### Step 2: Check for blockers

The constructs below have no direct equivalent in the typed SDK. If found, report them to the user and exclude the affected models from the mechanical migration:

- `bauplan.Model(...)` using `ref`, `connector`, `connector_config_key`, or `connector_config_uri`: the new `Model` is a dataclass accepting only `name`, `projection_schema`, `filter`.
- A `filter=` built from an f-string or variable: the new `filter` must be a string literal (`$param` templating inside the literal is fine).
- The legacy decorators `@bauplan.resources()`, `@bauplan.extras()` are not supported anymore and need to be removed. Confirm with user.
- The use of `filter=` on models now applies only to tables read from the catalog; when applied to other models (nodes in the DAG) it results in an error. This is not a hard-blocker since one can move the logic inside the body of the function.

### Step 3: Discover column types

The old `columns=[...]` lists carry names only; `TableSchema` classes need types.

- For source tables identified in the previous step, read the catalog: `bauplan table get <namespace>.<table>` on the reference branch.
- For model outputs, derive the type from the transformation and the upstream schemas.
- If a type cannot be determined from the catalog or the code, ask the user. Never invent a type.

Map catalog types to the field types listed in [examples.md](examples.md#5-declaring-tableschema-classes). There is no unsigned or 32-bit float field type; use `Float64` for floats and cast unsigned integers in the model body. Read the `REQUIRED` column too: a bare field type declares a required column, so every source column that is not required migrates to `Type | None`.

Nested types (list, struct, map) and decimals have no field type at all. Annotate those fields as `bauplan.Any`, the SDK's passthrough type rather than `typing.Any`: the column keeps its real type at runtime, the contract just does not pin it. This applies to output schemas and to `projection_schema` classes migrated from an old `columns=[...]` list, so a nested or decimal column is never a reason to drop a projection or a return annotation. Flag every `Any` field in the report.

### Step 4: Upgrade Python and the SDK dependency

The typed SDK requires Python >= 3.11 and the schema classes use `X | None` union syntax, so raising the interpreter is mandatory, not a preference: on an older interpreter the install will not resolve. Bump `requires-python` in `pyproject.toml` to `>=3.11` (and `uv python pin 3.11` if the project uses uv) before touching the SDK version.

Then check whether the project uses `uv` (`pyproject.toml` / `uv.lock`) or plain pip, and upgrade `bauplan` to the newest release. Do not trust the version number alone: verify the installed package actually ships the typed SDK by probing it:

```bash
uv run python -c "import bauplan; bauplan.TableSchema"
```

If this raises, the installed release predates the typed SDK (early 0.3.0 pre-releases do). Stop and ask the user which build to install; do not proceed with a package that lacks the symbols.

From now on run the CLI through the project environment (`uv run bauplan ...`) so the upgraded version is the one executing.

### Step 5: Rewrite

Apply the patterns from [examples.md](examples.md) to each file, in this order:

1. imports (pattern 1)
2. one `TableSchema` class per input projection and one per model output, with `| None` on every column the catalog does not mark required (patterns 3, 4, 5)
3. inputs from default arguments to `Annotated` (pattern 2)
4. return annotations on every model, `-> bool` on every expectation (patterns 4, 6)
5. parameters to `Annotated` (pattern 7)
6. return values converted to `pa.Table`, dtype casts where the schema pins a type (patterns 8, 9)
7. SQL models get the `output_schema` directive (pattern 10)

Respect the "do not over-migrate" list in [examples.md](examples.md#12-do-not-over-migrate): decorators, project yml, filters, and DAG semantics are untouched.

While rewriting, flag (do not delete) expectations that only assert a column's dtype: the declared schema now covers them, so propose replacing each with a value-level check.

### Step 6: Validate until green

Work on an isolated branch: `bauplan checkout -b <username>.<branch> --from-ref main`.

Validation ladder, from cheapest to most complete:

1. **Static checks**, only if installed (`which ruff` / `which ty`): `ruff check` and `ty check` (or `uv run ty check`) on the project. This is the payoff of the migration: the typed SDK ships stubs, so a wrong kwarg, a misspelled field type, or a schema class that does not resolve fails here in seconds, locally, before any job reaches the platform. Never skip it.
2. **Dry run**: `bauplan run --dry-run`. Schema contracts are enforced on every run, there is no flag to opt in, and strict mode is the default so a warning fails the run.
3. **Full run** on the isolated branch: `bauplan run`. Runtime dtype mismatches (unsigned counts, float widths) and extra output columns only surface here, as `ModelOutputContractError`.

Known error signatures and their causes:

- `A model cannot have input arguments without a default value: "<name>" found`, usually together with `A model must have at least one input model` (same pair for expectations), on lines you migrated: the environment does NOT understand the typed syntax yet, on the client side or on the backend side. The migrated code is fine. Do NOT "fix" it by reverting to default-argument inputs; re-check the SDK probe from step 4, then report to the user that their environment predates typed-SDK support and stop at static validation.
- `returns unknown schema "<Name>"`: the return annotation names a class that does not exist, is misspelled, or does not inherit directly from `TableSchema`.
- `Model must have a valid return type annotation`, or `Model return type annotation must be of the form: Annotated[pyarrow.Table, SchemaType]`: a model was left without a return annotation, or with a bare `-> pa.Table`. Every model needs a schema contract, so give it one instead of removing the annotation.
- `Unexpected TableField parameter: <name>`: `TableField` takes only `doc` and `lineage`. `title` and `nullable` are gone; nullability is `| None` on the type.
- `only the data type may be marked optional`: the `| None` was wrapped around the whole `Annotated[...]` instead of sitting on the data type inside it.
- `while parsing your code`: syntax-level problem in the rewritten file; re-read the corresponding pattern in examples.md.
- `ModelOutputContractError` on a full run: the returned table does not match the contract exactly, either because the output schema declares columns or types the function does not produce, or because the function returns a column the schema does not declare; fix the schema or add a cast (pattern 9).
- `ty` reporting unresolved imports for the model's runtime dependencies (duckdb, polars, local utils modules) is expected: those live in the remote model environment, not locally. Add them to the project's dev dependencies (`uv add --dev polars`) so the checker resolves them; the local install has no effect on the model environment declared by `@bauplan.python(pip={...})`. Unresolved `bauplan` symbols are real errors; unresolved third-party modules are not.

Iterate on errors until dry run and full run both pass. Then clean up: delete the validation branch if the user does not want to keep it.

### Step 7: Report

Summarize for the user: files rewritten, schema classes created (and which table or model each describes), parameters retyped, casts added, columns migrated to `| None`, lineage dropped because it pointed at the catalog, fields left as `Any` because nested types and decimals have no field type, expectations flagged as redundant, and any blockers left unmigrated with the reason.

## Workflow Checklist

- [ ] Step 1: Inventory all models, expectations, inputs, parameters, returns
- [ ] Step 2: Report blockers (`connector`/`ref` kwargs, non-literal filters)
- [ ] Step 3: Read source table schemas → `bauplan table get <namespace>.<table>`
- [ ] Step 4: Upgrade Python to >= 3.11 (**mandatory**), then the project dependency to the typed SDK (0.3.x)
- [ ] Step 5: Rewrite files following [examples.md](examples.md)
- [ ] Step 6: Create validation branch → `ty check` → dry run → full run → iterate until green
- [ ] Step 7: Report changes, flagged expectations, and leftovers to the user

## Reference

When unsure about a signature or concept, fetch the doc page via `WebFetch` rather than guessing. Pages are markdown and LLM-friendly.

**Python SDK:** `https://docs.bauplanlabs.com/reference/bauplan.md`

**Relevant concept pages:**
- Semantic annotations: `https://docs.bauplanlabs.com/concepts/semantic_annotations.md`
- Models: `https://docs.bauplanlabs.com/concepts/models.md`
- Expectations: `https://docs.bauplanlabs.com/concepts/expectations.md`
- Parameters: `https://docs.bauplanlabs.com/common-scenarios/parameterized-runs.md`

**CLI:** self-documenting via `bauplan --help` and `bauplan <command> --help`.
