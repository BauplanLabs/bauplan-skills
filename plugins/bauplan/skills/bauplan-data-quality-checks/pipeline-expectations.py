"""
Pipeline expectations for an e-commerce analytics pipeline.

This file demonstrates expectations for a pipeline with the following DAG:

    [raw_ecommerce_events] → [staging] → [session_metrics] → [daily_summary]

Each expectation documents:
  - What it checks and why (the assumption)
  - Which downstream consumer depends on it
  - The severity (assert = FAIL, print = WARN)

Place this file as expectations.py in the pipeline project directory,
alongside models.py and bauplan_project.yml.
"""

import bauplan

# ==========================================================================
# Expectations on: staging
# Consumer: session_metrics model (groups by user_session, aggregates price)
# ==========================================================================


@bauplan.expectation()
@bauplan.python("3.11")
def test_staging_no_null_sessions(data=bauplan.Model("staging", columns=["user_session"])):
    """
    user_session must not be null — session_metrics groups by it.
    Severity: FAIL (null sessions produce orphan rows that silently drop from aggregations).
    """
    from bauplan.standard_expectations import expect_column_no_nulls

    result = expect_column_no_nulls(data, "user_session")
    assert result, "user_session contains null values — session_metrics will lose rows"
    return result


@bauplan.expectation()
@bauplan.python("3.11")
def test_staging_no_null_event_time(data=bauplan.Model("staging", columns=["event_time"])):
    """
    event_time must not be null — session_metrics computes session_start/session_end from it.
    Severity: FAIL (null timestamps break MIN/MAX aggregations).
    """
    from bauplan.standard_expectations import expect_column_no_nulls

    result = expect_column_no_nulls(data, "event_time")
    assert result, "event_time contains null values — session time windows will be wrong"
    return result


@bauplan.expectation()
@bauplan.python("3.11")
def test_staging_valid_event_types(data=bauplan.Model("staging", columns=["event_type"])):
    """
    event_type must be one of the known types — session_metrics filters on event_type='purchase'.
    Severity: FAIL (unknown types could be mis-categorized purchases that inflate or deflate revenue).
    """
    from bauplan.standard_expectations import expect_column_accepted_values

    result = expect_column_accepted_values(data, "event_type", ["view", "cart", "purchase", "remove"])
    assert result, "event_type contains unexpected values"
    return result


@bauplan.expectation()
@bauplan.python("3.11")
def test_staging_positive_prices(data=bauplan.Model("staging", columns=["price"])):
    """
    price should not be negative — session_metrics sums it for session_revenue.
    Severity: WARN (a few negative prices from refunds are possible but unusual;
    investigate if this fires, but don't halt the pipeline).
    """
    from bauplan.standard_expectations import expect_column_mean_greater_than

    result = expect_column_mean_greater_than(data, "price", 0.0)
    if not result:
        print("WARNING: average price is <= 0 — check for refund contamination")
    return result


@bauplan.expectation()
@bauplan.python("3.11")
def test_staging_minimum_rows(data=bauplan.Model("staging")):
    """
    staging must have a meaningful number of rows — fewer than 100 indicates
    a broken upstream source or overly aggressive filter.
    Severity: FAIL (downstream aggregations on tiny datasets are meaningless).
    """
    row_count = data.num_rows
    is_sufficient = row_count >= 100
    assert is_sufficient, f"staging has only {row_count} rows — expected at least 100"
    return is_sufficient


# ==========================================================================
# Expectations on: session_metrics
# Consumer: daily_summary model (groups by date, sums purchases and revenue)
# ==========================================================================


@bauplan.expectation()
@bauplan.python("3.11")
def test_sessions_unique(data=bauplan.Model("session_metrics", columns=["user_session"])):
    """
    user_session must be unique in session_metrics — it's the grain of the table.
    Severity: FAIL (duplicate sessions double-count revenue in daily_summary).
    """
    from bauplan.standard_expectations import expect_column_all_unique

    result = expect_column_all_unique(data, "user_session")
    assert result, "user_session has duplicates — daily_summary revenue will be inflated"
    return result


@bauplan.expectation()
@bauplan.python("3.11")
def test_sessions_no_null_revenue(data=bauplan.Model("session_metrics", columns=["session_revenue"])):
    """
    session_revenue must not be null — daily_summary sums it.
    Severity: FAIL (null revenue values cause SUM to silently exclude rows).
    """
    from bauplan.standard_expectations import expect_column_no_nulls

    result = expect_column_no_nulls(data, "session_revenue")
    assert result, "session_revenue contains nulls — daily totals will undercount"
    return result


# ==========================================================================
# Expectations on: daily_summary (final output)
# Consumer: external dashboard / BI tool
# ==========================================================================


@bauplan.expectation()
@bauplan.python("3.11", pip={"polars": "1.15.0"})
def test_daily_summary_freshness(data=bauplan.Model("daily_summary", columns=["date"])):
    """
    Most recent date must be within 3 days of today — the executive dashboard
    shows daily trends and stale data causes incorrect business decisions.
    Severity: WARN (stale data is bad but not corrupting; may just mean
    the source hasn't delivered yet).
    """
    from datetime import datetime, timedelta

    import polars as pl

    df = pl.from_arrow(data)
    max_date = df.select(pl.col("date").max()).item()
    threshold = datetime.now() - timedelta(days=3)
    is_fresh = max_date >= threshold
    if not is_fresh:
        print(f"WARNING: daily_summary is stale — most recent date is {max_date}")
    return is_fresh


@bauplan.expectation()
@bauplan.python("3.11")
def test_daily_summary_no_null_dates(data=bauplan.Model("daily_summary", columns=["date"])):
    """
    date must not be null — it's the primary key of the summary table.
    Severity: FAIL (null dates make rows invisible in time-based dashboards).
    """
    from bauplan.standard_expectations import expect_column_no_nulls

    result = expect_column_no_nulls(data, "date")
    assert result, "daily_summary has null dates"
    return result


@bauplan.expectation()
@bauplan.python("3.11")
def test_daily_summary_reasonable_conversion(
    data=bauplan.Model("daily_summary", columns=["conversion_rate"]),
):
    """
    Average conversion rate should be below 100% — values above that indicate
    a calculation bug (more purchases than sessions is impossible).
    Severity: FAIL (this means the pipeline logic is wrong, not just bad data).
    """
    from bauplan.standard_expectations import expect_column_mean_smaller_than

    result = expect_column_mean_smaller_than(data, "conversion_rate", 100.0)
    assert result, "conversion_rate exceeds 100% — calculation bug in session_metrics"
    return result
