"""Expectation suites per lakehouse layer.

Each layer exposes a builder that returns a list of GX 1.x expectation
instances. Suites are intentionally small and high-signal: each broken
assertion should point to a real data integrity issue, not a stylistic
preference.
"""

from __future__ import annotations

from typing import Any

import great_expectations.expectations as gxe

#: Top GH Archive event types (covers >95% of public traffic). Other types are
#: tolerated via the ``mostly`` knob on the bronze-layer membership check.
KNOWN_EVENT_TYPES: tuple[str, ...] = (
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "WatchEvent",
    "ForkEvent",
    "CreateEvent",
    "DeleteEvent",
    "IssueCommentEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
    "ReleaseEvent",
    "PublicEvent",
    "MemberEvent",
    "GollumEvent",
    "CommitCommentEvent",
)


def bronze_expectations() -> list[Any]:
    """Bronze is raw — only the bare-minimum invariants we need downstream."""
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="id"),
        gxe.ExpectColumnValuesToNotBeNull(column="type"),
        gxe.ExpectColumnValuesToNotBeNull(column="created_at"),
        gxe.ExpectColumnValuesToBeInSet(
            column="type",
            value_set=list(KNOWN_EVENT_TYPES),
            mostly=0.95,
        ),
    ]


def silver_expectations() -> list[Any]:
    """Silver is the contract: schema enforced, deduped, partitioned."""
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
        gxe.ExpectColumnValuesToBeUnique(column="event_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="event_type"),
        gxe.ExpectColumnValuesToNotBeNull(column="created_at"),
        gxe.ExpectColumnValuesToNotBeNull(column="event_date"),
        gxe.ExpectColumnValuesToBeBetween(column="event_hour", min_value=0, max_value=23),
    ]


def gold_expectations() -> list[Any]:
    """Gold fact: dbt already tests uniqueness/referential integrity, this is
    a second independent check from a different framework."""
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="event_id"),
        gxe.ExpectColumnValuesToBeUnique(column="event_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="event_date"),
        gxe.ExpectColumnValuesToNotBeNull(column="event_type"),
    ]


SUITE_BUILDERS: dict[str, Any] = {
    "bronze": bronze_expectations,
    "silver": silver_expectations,
    "gold": gold_expectations,
}
