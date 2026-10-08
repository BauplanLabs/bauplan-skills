"""
Pipeline expectations for an e-commerce analytics pipeline.

This file demonstrates expectations for a pipeline with the following DAG:

    [raw_ecommerce_events] → [staging] → [session_metrics] → [daily_summary]

Each expectation documents:
  - What it checks and why (the assumption)
  - Which downstream consumer depends on it
  - Why a failure is worth stopping the run

Every expectation returns a bool. Strict mode is the default, so a False
return fails the job.

Place this file as expectations.py in the pipeline project directory,
alongside models.py and bauplan_project.yml.
"""

from typing import Annotated

import bauplan
import pyarrow as pa


# Verify source contracts before adapting these projections to another pipeline
class SessionIdExpectationColumns(bauplan.TableSchema):
    """Session identifier used for completeness and uniqueness checks."""

    user_session: bauplan.String | None


class EventTimeExpectationColumns(bauplan.TableSchema):
    """Event timestamp used to validate session time inputs."""

    event_time: bauplan.TimestampMicro | None


class EventTypeExpectationColumns(bauplan.TableSchema):
    """Event category checked against accepted values."""

    event_type: bauplan.String | None


class PriceExpectationColumns(bauplan.TableSchema):
    """Staged decimal price checked before session aggregation."""

    price: bauplan.Any  # decimal column, no schema type available


class SessionRevenueExpectationColumns(bauplan.TableSchema):
    """Aggregated decimal revenue checked before daily summarization."""

    session_revenue: bauplan.Any  # decimal column, no schema type available


class SummaryDateExpectationColumns(bauplan.TableSchema):
    """Daily timestamp checked for freshness and completeness."""

    date: bauplan.TimestampMicro | None


class ConversionRateExpectationColumns(bauplan.TableSchema):
    """Daily conversion metric checked against its expected bound."""

    conversion_rate: bauplan.Float64 | None


# ==========================================================================
# Expectations on: staging
# Consumer: session_metrics model (groups by user_session, aggregates price)
# ==========================================================================


@bauplan.expectation()
@bauplan.python("3.11")
def test_staging_no_null_sessions(
    data: Annotated[
        pa.Table,
        bauplan.Model("staging", projection_schema=SessionIdExpectationColumns),
    ],
) -> bool:
    """
    user_session must not be null — session_metrics groups by it.
    Impact: null sessions produce orphan rows that silently drop from aggregations.
    """
    from bauplan.standard_expectations import expect_column_no_nulls

    return expect_column_no_nulls(data, "user_session")


@bauplan.expectation()
@bauplan.python("3.11")
def test_staging_no_null_event_time(
    data: Annotated[
        pa.Table,
        bauplan.Model("staging", projection_schema=EventTimeExpectationColumns),
    ],
) -> bool:
    """
    event_time must not be null — session_metrics computes session_start/session_end from it.
    Impact: null timestamps break MIN/MAX aggregations.
    """
    from bauplan.standard_expectations import expect_column_no_nulls

    return expect_column_no_nulls(data, "event_time")


@bauplan.expectation()
@bauplan.python("3.11")
def test_staging_valid_event_types(
    data: Annotated[
        pa.Table,
        bauplan.Model("staging", projection_schema=EventTypeExpectationColumns),
    ],
) -> bool:
    """
    event_type must be one of the known types — session_metrics filters on event_type='purchase'.
    Impact: unknown types could be mis-categorized purchases that inflate or deflate revenue.
    """
    from bauplan.standard_expectations import expect_column_accepted_values

    return expect_column_accepted_values(
        data, "event_type", ["view", "cart", "purchase", "remove"]
    )


@bauplan.expectation()
@bauplan.python("3.11")
def test_staging_positive_prices(
    data: Annotated[
        pa.Table,
        bauplan.Model("staging", projection_schema=PriceExpectationColumns),
    ],
) -> bool:
    """
    Mean price must be positive — session_metrics sums price for session_revenue.
    Refunds make a few negative prices normal, so the check is on the mean.
    Impact: session_revenue and every daily total built on it would be wrong.
    """
    from bauplan.standard_expectations import expect_column_mean_greater_than

    return expect_column_mean_greater_than(data, "price", 0.0)


@bauplan.expectation()
@bauplan.python("3.11")
def test_staging_minimum_rows(
    data: Annotated[pa.Table, bauplan.Model("staging")],
) -> bool:
    """
    staging must have a meaningful number of rows — fewer than 100 indicates
    a broken upstream source or overly aggressive filter.
    Impact: downstream aggregations on tiny datasets are meaningless.
    """
    row_count = data.num_rows
    return row_count >= 100


# ==========================================================================
# Expectations on: session_metrics
# Consumer: daily_summary model (groups by date, sums purchases and revenue)
# ==========================================================================


@bauplan.expectation()
@bauplan.python("3.11")
def test_sessions_unique(
    data: Annotated[
        pa.Table,
        bauplan.Model("session_metrics", projection_schema=SessionIdExpectationColumns),
    ],
) -> bool:
    """
    user_session must be unique in session_metrics — it's the grain of the table.
    Impact: duplicate sessions double-count revenue in daily_summary.
    """
    from bauplan.standard_expectations import expect_column_all_unique

    return expect_column_all_unique(data, "user_session")


@bauplan.expectation()
@bauplan.python("3.11")
def test_sessions_no_null_revenue(
    data: Annotated[
        pa.Table,
        bauplan.Model(
            "session_metrics", projection_schema=SessionRevenueExpectationColumns
        ),
    ],
) -> bool:
    """
    session_revenue must not be null — daily_summary sums it.
    Impact: null revenue values cause SUM to silently exclude rows.
    """
    from bauplan.standard_expectations import expect_column_no_nulls

    return expect_column_no_nulls(data, "session_revenue")


# ==========================================================================
# Expectations on: daily_summary (final output)
# Consumer: external dashboard / BI tool
# ==========================================================================


@bauplan.expectation()
@bauplan.python("3.11", pip={"polars": "1.15.0"})
def test_daily_summary_freshness(
    data: Annotated[
        pa.Table,
        bauplan.Model("daily_summary", projection_schema=SummaryDateExpectationColumns),
    ],
) -> bool:
    """
    Most recent date must be within 3 days of today — the executive dashboard
    shows daily trends and stale data causes incorrect business decisions.
    Impact: a stale summary misleads every reader of the dashboard.
    """
    from datetime import datetime, timedelta

    import polars as pl

    df = pl.DataFrame(data)
    max_date = df.select(pl.col("date").max()).item()
    # The schema declares a timezone-naive timestamp, so compare local wall times
    threshold = datetime.now() - timedelta(days=3)  # noqa: DTZ005
    return max_date >= threshold


@bauplan.expectation()
@bauplan.python("3.11")
def test_daily_summary_no_null_dates(
    data: Annotated[
        pa.Table,
        bauplan.Model("daily_summary", projection_schema=SummaryDateExpectationColumns),
    ],
) -> bool:
    """
    date must not be null — it's the primary key of the summary table.
    Impact: null dates make rows invisible in time-based dashboards.
    """
    from bauplan.standard_expectations import expect_column_no_nulls

    return expect_column_no_nulls(data, "date")


@bauplan.expectation()
@bauplan.python("3.11")
def test_daily_summary_reasonable_conversion(
    data: Annotated[
        pa.Table,
        bauplan.Model(
            "daily_summary", projection_schema=ConversionRateExpectationColumns
        ),
    ],
) -> bool:
    """
    Average conversion rate must be at most 100%, including days when every session converts.
    Impact: this means the pipeline logic is wrong, not just bad data.
    """
    from bauplan.standard_expectations import expect_column_mean_smaller_or_equal_than

    return expect_column_mean_smaller_or_equal_than(data, "conversion_rate", 100.0)
