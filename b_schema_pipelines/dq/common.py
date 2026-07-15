"""
Shared helper for the Bronze/Silver/Gold Great Expectations suite factories.

GX 1.x's `ExpectationSuite.add_expectation()` requires an active data context
(it checks whether the suite has been persisted) — an ephemeral, in-memory
context satisfies that without writing anything to disk or a GX Cloud
project, which is all a suite-construction factory needs.
"""

from __future__ import annotations

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite


def new_suite(name: str) -> ExpectationSuite:
    """Create an empty, addable ExpectationSuite backed by an ephemeral context."""
    context = gx.get_context(mode="ephemeral")
    return context.suites.add(ExpectationSuite(name=name))
