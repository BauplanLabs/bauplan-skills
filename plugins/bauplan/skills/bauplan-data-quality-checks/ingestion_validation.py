"""
Ingestion validation for a safe ingestion using a WAP (Write-Audit-Publish) workflow.

This file demonstrates validation logic embedded in an ingestion script
that imports order data from S3. The validate_import() function runs
between import_data() and merge_branch().

Each check documents:
  - What it checks and why (the assumption)
  - Which downstream consumer depends on it
  - The severity (assert = FAIL blocks merge, print = WARN logs only)

This is not a standalone script — it shows the validation function
and how it integrates into the WAP flow. See the bauplan-safe-ingestion skill
for the full WAP script template.
"""

from datetime import datetime, timedelta

import bauplan


def validate_import(
    client: "bauplan.Client",
    table_name: str,
    branch: str,
    namespace: str = "bauplan",
):
    """
    Quality-gate checks on an imported table before merging to main.

    Raises AssertionError on FAIL checks → blocks merge, preserves branch.
    Prints warnings for WARN checks → logs concern, does not block.

    Args:
        client: Authenticated bauplan client
        table_name: Name of the imported table
        branch: Import branch to validate against
        namespace: Table namespace (default: bauplan)
    """
    fq_table = f"{namespace}.{table_name}"
    print(f"Validating {fq_table} on branch {branch}...")

    # ==================================================================
    # Volume: table must exist and have rows
    # Consumer: the billing pipeline reads this table daily
    # Severity: FAIL — an empty table means the source feed is broken
    # ==================================================================

    assert client.has_table(table=table_name, ref=branch, namespace=namespace), (
        f"Table {fq_table} does not exist on branch {branch}"
    )

    result = client.query(f"SELECT COUNT(*) as n FROM {fq_table}", ref=branch)
    row_count = result.column("n")[0].as_py()
    assert row_count > 0, f"Table {fq_table} has 0 rows after import"
    print(f"  ✓ Volume: {row_count} rows")

    # ==================================================================
    # Schema: expected columns must be present
    # Consumer: billing pipeline selects order_id, customer_id, total, order_date
    # Severity: FAIL — missing columns cause the downstream pipeline to crash
    # ==================================================================

    table_meta = client.get_table(table=table_name, ref=branch, namespace=namespace)
    actual_columns = {f.name for f in table_meta.fields}
    expected_columns = {"order_id", "customer_id", "total", "order_date"}
    missing = expected_columns - actual_columns
    assert not missing, f"Missing expected columns: {missing}"
    print("  ✓ Schema: all expected columns present")

    # ==================================================================
    # Completeness: join keys must not be null
    # Consumer: billing pipeline joins orders to customers on customer_id,
    #           and uses order_id as primary key for deduplication
    # Severity: FAIL — null keys produce orphan rows and silent data loss
    # ==================================================================

    for col in ["order_id", "customer_id"]:
        result = client.query(
            f"SELECT COUNT(*) - COUNT({col}) as nulls FROM {fq_table}",
            ref=branch,
        )
        null_count = result.column("nulls")[0].as_py()
        assert null_count == 0, f"{col} has {null_count} null values"
    print("  ✓ Completeness: order_id and customer_id have no nulls")

    # ==================================================================
    # Uniqueness: order_id is the grain of the table
    # Consumer: billing pipeline counts distinct orders for revenue reporting
    # Severity: FAIL — duplicate order_ids inflate revenue
    # ==================================================================

    result = client.query(
        f"SELECT COUNT(order_id) - COUNT(DISTINCT order_id) as dupes FROM {fq_table}",
        ref=branch,
    )
    dupe_count = result.column("dupes")[0].as_py()
    assert dupe_count == 0, f"order_id has {dupe_count} duplicates"
    print("  ✓ Uniqueness: order_id is unique")

    # ==================================================================
    # Validity: total must be non-negative
    # Consumer: revenue model sums total — negatives corrupt the output
    # Severity: FAIL for any negative values
    # ==================================================================

    result = client.query(
        f"SELECT MIN(total) as min_total, MAX(total) as max_total FROM {fq_table}",
        ref=branch,
    )
    min_total = result.column("min_total")[0].as_py()
    assert min_total is None or min_total >= 0, f"total has negative values (min: {min_total})"
    print("  ✓ Validity: all totals are non-negative")

    # ==================================================================
    # Freshness: most recent order should be recent
    # Consumer: daily dashboard shows orders by date — stale data misleads ops team
    # Severity: WARN — stale data is concerning but not corrupting;
    #           the source may simply not have delivered yet
    # ==================================================================

    result = client.query(
        f"SELECT MAX(order_date) as latest FROM {fq_table}",
        ref=branch,
    )
    latest = result.column("latest")[0].as_py()
    if latest is not None:
        # Compare against 3-day threshold — adjust to match source SLA
        threshold = datetime.now() - timedelta(days=3)
        if latest < threshold:
            print(f"  ⚠ Freshness: most recent order_date is {latest} (older than {threshold.date()})")
        else:
            print(f"  ✓ Freshness: most recent order_date is {latest}")
    else:
        print("  ⚠ Freshness: order_date is entirely null — cannot assess")

    # ==================================================================
    # Consistency: order_date should not be in the future
    # Consumer: time-series dashboards — future dates create phantom data points
    # Severity: WARN — likely a timezone or ETL bug, not data corruption
    # ==================================================================

    result = client.query(
        f"SELECT COUNT(*) as n FROM {fq_table} WHERE order_date > CURRENT_DATE",
        ref=branch,
    )
    future_count = result.column("n")[0].as_py()
    if future_count > 0:
        print(f"  ⚠ Consistency: {future_count} rows have order_date in the future")
    else:
        print("  ✓ Consistency: no future-dated orders")

    print(f"Validation passed for {fq_table}")


# ======================================================================
# Integration into WAP script
# ======================================================================
#
# In your quality_gated_update() function, call validate_import()
# between the import and merge phases:
#
#     # === IMPORT PHASE ===
#     client.create_table(table=table_name, search_uri=s3_path, ...)
#     client.import_data(table=table_name, search_uri=s3_path, ...)
#
#     # === VALIDATION PHASE ===
#     validate_import(client, table_name, branch_name, namespace)
#     # If validate_import raises, merge is skipped and branch is preserved.
#
#     # === MERGE PHASE ===
#     if on_success == "merge":
#         client.merge_branch(source_ref=branch_name, into_branch="main")
#
# The try/except in the WAP template catches AssertionError from
# validate_import and routes to the on_failure behavior (inspect or delete).
