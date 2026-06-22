# ruff: noqa: E402  -- sys.path manipulation must precede project imports
from pathlib import Path
import sys
import pytest
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[2]))

from a_data_generator.generator import DataGenerator
from config.settings import settings

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def generator():
    return DataGenerator(settings.TEST_DATA_GENERATOR_CONFIG_PATH)


@pytest.fixture(scope="module")
def generated_data(generator):
    customers  = generator._generate_customers(generator.config)
    products   = generator._generate_products(generator.config)
    orders     = generator._generate_orders(customers, generator.config)
    order_items= generator._generate_order_items(orders, products, generator.config)
    payments   = generator._generate_payments(orders, order_items, generator.config)
    return {
        "customers":   customers.df,
        "products":    products.df,
        "orders":      orders.df,
        "order_items": order_items.df,
        "payments":    payments.df,
    }


@pytest.fixture(scope="module")
def stream_events(generator, generated_data):
    """Generate streaming events — module-scoped so they're built once."""
    return generator.generate_stream_events(
        generated_data["customers"],
        generated_data["products"],
    )


@pytest.fixture(scope="module")
def events_df(stream_events):
    df = pd.DataFrame(stream_events)
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    df["created_ts"]      = pd.to_datetime(df["created_ts"])
    return df


# ---------------------------------------------------------------------------
# 1. Row Counts / Volume
# ---------------------------------------------------------------------------

def test_customer_count(generated_data, generator):
    assert len(generated_data["customers"]) == generator.config["n_customers"]


def test_product_count(generated_data, generator):
    assert len(generated_data["products"]) == generator.config["n_products"]


def test_order_count_approx(generated_data, generator):
    """Orders follow Poisson(λ = avg_orders_per_customer), so total ≈ n × λ ±15%."""
    expected = generator.config["n_customers"] * generator.config["avg_orders_per_customer"]
    actual   = len(generated_data["orders"])
    assert 0.85 * expected <= actual <= 1.15 * expected, (
        f"order count {actual:,} outside ±15% of expected {expected:,.0f}"
    )


def test_order_items_count_approx(generated_data, generator):
    """order_items ≈ n_orders × avg_items_per_order × (1 + dup_rate) ±15%."""
    n_orders  = len(generated_data["orders"])
    dup_rate  = generator.config["duplicate_rate_offline"]
    expected  = n_orders * generator.config["avg_items_per_order"] * (1 + dup_rate)
    actual    = len(generated_data["order_items"])
    assert 0.85 * expected <= actual <= 1.15 * expected, (
        f"order_items count {actual:,} outside ±15% of expected {expected:,.0f}"
    )


def test_payments_one_per_order(generated_data):
    """Each order must have exactly one payment row."""
    n_orders   = len(generated_data["orders"])
    n_payments = len(generated_data["payments"])
    assert n_orders == n_payments, (
        f"payments ({n_payments:,}) != orders ({n_orders:,})"
    )


# ---------------------------------------------------------------------------
# 2. Schema — Required Columns
# ---------------------------------------------------------------------------

EXPECTED_COLUMNS = {
    "customers":   {"customer_id", "signup_ts", "country", "segment", "marketing_opt_in"},
    "products":    {"product_id", "category", "brand", "base_price", "is_active", "created_ts"},
    "orders":      {"order_id", "customer_id", "order_timestamp", "status",
                    "shipping_city", "shipping_method", "coupon_code"},
    "order_items": {"order_item_id", "order_id", "product_id", "quantity",
                    "unit_price", "discount"},
    "payments":    {"payment_id", "order_id", "payment_timestamp",
                    "payment_method", "amount", "payment_status"},
}

@pytest.mark.parametrize("table,cols", EXPECTED_COLUMNS.items())
def test_columns_present(generated_data, table, cols):
    missing = cols - set(generated_data[table].columns)
    assert not missing, f"{table}: missing columns {missing}"


EXPECTED_EVENT_COLUMNS = {
    "event_id", "event_type", "event_timestamp", "created_ts",
    "customer_id", "session_id", "product_id", "order_id", "quantity", "price",
}

def test_event_schema(events_df):
    missing = EXPECTED_EVENT_COLUMNS - set(events_df.columns)
    assert not missing, f"events: missing columns {missing}"


# ---------------------------------------------------------------------------
# 3. Primary Key Uniqueness
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table,pk", [
    ("customers",   "customer_id"),
    ("products",    "product_id"),
    ("orders",      "order_id"),
    ("payments",    "payment_id"),
])
def test_primary_key_unique(generated_data, table, pk):
    df = generated_data[table]
    assert df[pk].is_unique, f"{table}.{pk} has duplicate values"


def test_order_item_id_unique_after_dedup(generated_data):
    """After dedup by natural key, no duplicate rows must remain.

    order_item_id is NOT unique in the raw source — injected duplicates are exact
    row copies (same ID). Uniqueness is enforced at the Silver layer. This test
    validates that the natural-key dedup is sufficient to restore uniqueness.
    """
    df  = generated_data["order_items"]
    key = ["order_id", "product_id", "quantity", "unit_price"]
    deduped = df.drop_duplicates(subset=key)
    assert not deduped.duplicated(subset=key).any(), (
        "Natural key still has duplicates after dedup — dedup logic is broken"
    )


# ---------------------------------------------------------------------------
# 4. Problem A — Geographic Skew (compulsory)
# ---------------------------------------------------------------------------

def test_hcmc_skew(generated_data, generator):
    """85% of orders must ship from Ho Chi Minh City ±5%."""
    rate = (generated_data["orders"]["shipping_city"] == "Ho Chi Minh City").mean()
    target = generator.config["skew_ratio_city"]
    assert abs(rate - target) <= 0.05, (
        f"HCMC rate {rate:.1%} deviates from target {target:.0%} by more than 5%"
    )


def test_electronics_skew(generated_data, generator):
    """80% of products must be electronics ±8%."""
    rate   = (generated_data["products"]["category"] == "electronics").mean()
    target = generator.config["skew_ratio_category"]
    assert abs(rate - target) <= 0.08, (
        f"Electronics rate {rate:.1%} deviates from target {target:.0%} by more than 8%"
    )


def test_only_valid_cities(generated_data, generator):
    actual   = set(generated_data["orders"]["shipping_city"].dropna().unique())
    expected = set(generator.config["cities"])
    assert actual.issubset(expected), f"Unexpected cities: {actual - expected}"


# ---------------------------------------------------------------------------
# 5. Problem B — Schema Evolution (compulsory)
# ---------------------------------------------------------------------------

def test_schema_evolution_old_orders_null(generated_data, generator):
    """Orders before schema_change_date must have NULL coupon_code and shipping_method."""
    orders = generated_data["orders"]
    cut    = generator.config["schema_change_date"]
    old    = orders[orders["order_timestamp"] < cut]
    assert old["coupon_code"].isna().all(),     "Old orders: expected coupon_code = NULL"
    assert old["shipping_method"].isna().all(), "Old orders: expected shipping_method = NULL"


def test_schema_evolution_new_orders_populated(generated_data, generator):
    """Orders after schema_change_date must have non-NULL shipping_method."""
    orders = generated_data["orders"]
    cut    = generator.config["schema_change_date"]
    new    = orders[orders["order_timestamp"] >= cut]
    assert new["shipping_method"].notna().any(), "New orders: expected non-NULL shipping_method"


def test_schema_evolution_both_partitions_exist(generated_data, generator):
    """Both old and new partitions must exist (schema_change_date is mid-window)."""
    orders = generated_data["orders"]
    cut    = generator.config["schema_change_date"]
    old_ratio = (orders["order_timestamp"] < cut).mean()
    assert 0.10 <= old_ratio <= 0.90, (
        f"Old partition is {old_ratio:.0%} of orders — schema_change_date may be outside the window"
    )


# ---------------------------------------------------------------------------
# 6. Problem C — Duplicate Order Items (optional, chosen)
# ---------------------------------------------------------------------------

def test_duplicate_rate(generated_data, generator):
    """Quality-report duplicate rate (keep=False) must match config ±1%.

    The quality report uses duplicated(keep=False) which marks both the original
    and the injected copy, so the measured rate ≈ 2 × injected_fraction.
    The generator injects dup_rate/2 rows so this measurement hits the target.
    """
    df  = generated_data["order_items"]
    key = ["order_id", "product_id", "quantity", "unit_price"]
    rate     = df.duplicated(subset=key, keep=False).mean()
    expected = generator.config["duplicate_rate_offline"]
    assert abs(rate - expected) < 0.01, (
        f"Duplicate rate {rate:.2%} differs from expected {expected:.2%} by more than 1%"
    )


def test_duplicate_natural_key(generated_data):
    """Duplicates share the same (order_id, product_id, quantity, unit_price)."""
    df  = generated_data["order_items"]
    key = ["order_id", "product_id", "quantity", "unit_price"]
    dup_rows = df[df.duplicated(subset=key, keep=False)]
    assert len(dup_rows) > 0, "No duplicates found — injection may have failed"


# ---------------------------------------------------------------------------
# 7. Distribution Tests
# ---------------------------------------------------------------------------

def _check_dist(series, expected: dict, tol: float, label: str):
    actual = series.value_counts(normalize=True)
    for value, exp_pct in expected.items():
        act_pct = actual.get(value, 0)
        assert abs(act_pct - exp_pct) <= tol, (
            f"{label}: {value!r} is {act_pct:.1%}, expected {exp_pct:.0%} ±{tol:.0%}"
        )


def test_customer_segment_distribution(generated_data, generator):
    cfg = generator.config["customer_segment_distribution"]
    _check_dist(generated_data["customers"]["segment"], cfg, tol=0.08, label="customer.segment")


def test_marketing_opt_in_rate(generated_data, generator):
    rate   = generated_data["customers"]["marketing_opt_in"].mean()
    target = generator.config["marketing_opt_in_rate"]
    assert abs(rate - target) <= 0.05, (
        f"marketing_opt_in {rate:.1%} deviates from {target:.0%} by more than 5%"
    )


def test_payment_status_distribution(generated_data, generator):
    cfg = generator.config["payment_status_distribution"]
    _check_dist(generated_data["payments"]["payment_status"], cfg, tol=0.08, label="payment.status")


def test_payment_method_distribution(generated_data, generator):
    cfg = generator.config["payment_method_distribution"]
    _check_dist(generated_data["payments"]["payment_method"], cfg, tol=0.08, label="payment.method")


def test_order_status_distribution(generated_data, generator):
    cfg = generator.config["order_status_distribution"]
    _check_dist(generated_data["orders"]["status"], cfg, tol=0.08, label="order.status")


def test_only_valid_categories(generated_data, generator):
    actual   = set(generated_data["products"]["category"].unique())
    expected = set(generator.config["product_categories"])
    assert actual.issubset(expected), f"Unexpected categories: {actual - expected}"


def test_only_valid_segments(generated_data, generator):
    actual   = set(generated_data["customers"]["segment"].unique())
    expected = set(generator.config["customer_segments"])
    assert actual.issubset(expected), f"Unexpected segments: {actual - expected}"


def test_event_type_distribution(events_df, generator):
    cfg = generator.config["event_type_distribution"]
    _check_dist(events_df["event_type"], cfg, tol=0.05, label="event.type")


# ---------------------------------------------------------------------------
# 8. Timestamp Validity
# ---------------------------------------------------------------------------

def test_signup_ts_within_sim_window(generated_data, generator):
    ts = pd.to_datetime(generated_data["customers"]["signup_ts"])
    assert (ts >= generator.config["sim_start"]).all(), "signup_ts before sim_start"
    assert (ts <= generator.config["sim_end"]).all(),   "signup_ts after sim_end"


def test_order_timestamp_after_customer_signup(generated_data):
    """Every order must be placed after the customer signed up."""
    orders    = generated_data["orders"].copy()
    customers = generated_data["customers"][["customer_id", "signup_ts"]].copy()
    customers["signup_ts"] = pd.to_datetime(customers["signup_ts"])
    orders["order_timestamp"] = pd.to_datetime(orders["order_timestamp"])
    merged = orders.merge(customers, on="customer_id", how="left")
    violations = merged[merged["order_timestamp"] < merged["signup_ts"]]
    assert len(violations) == 0, (
        f"{len(violations):,} orders placed before customer signup"
    )


def test_payment_timestamp_after_order(generated_data):
    """Every payment must be recorded after its order was placed."""
    payments = generated_data["payments"].copy()
    orders   = generated_data["orders"][["order_id", "order_timestamp"]].copy()
    payments["payment_timestamp"] = pd.to_datetime(payments["payment_timestamp"])
    orders["order_timestamp"]     = pd.to_datetime(orders["order_timestamp"])
    merged = payments.merge(orders, on="order_id", how="left")
    violations = merged[merged["payment_timestamp"] < merged["order_timestamp"]]
    assert len(violations) == 0, (
        f"{len(violations):,} payments recorded before order_timestamp"
    )


def test_event_timestamps_same_day(events_df):
    """All events must fall within a single 24-hour day."""
    dates = events_df["event_timestamp"].dt.date.unique()
    assert len(dates) == 1, f"Events span {len(dates)} dates — expected exactly 1"


# ---------------------------------------------------------------------------
# 9. Referential Integrity
# ---------------------------------------------------------------------------

def test_orders_customer_fk(generated_data):
    valid = set(generated_data["customers"]["customer_id"])
    orphans = ~generated_data["orders"]["customer_id"].isin(valid)
    assert orphans.sum() == 0, f"{orphans.sum():,} orders with unknown customer_id"


def test_order_items_order_fk(generated_data):
    valid = set(generated_data["orders"]["order_id"])
    orphans = ~generated_data["order_items"]["order_id"].isin(valid)
    assert orphans.sum() == 0, f"{orphans.sum():,} order_items with unknown order_id"


def test_order_items_product_fk(generated_data):
    valid = set(generated_data["products"]["product_id"])
    orphans = ~generated_data["order_items"]["product_id"].isin(valid)
    assert orphans.sum() == 0, f"{orphans.sum():,} order_items with unknown product_id"


def test_payments_order_fk(generated_data):
    valid = set(generated_data["orders"]["order_id"])
    orphans = ~generated_data["payments"]["order_id"].isin(valid)
    assert orphans.sum() == 0, f"{orphans.sum():,} payments with unknown order_id"


def test_events_customer_fk(events_df, generated_data):
    valid = set(generated_data["customers"]["customer_id"])
    orphans = ~events_df["customer_id"].isin(valid)
    assert orphans.sum() == 0, f"{orphans.sum():,} events with unknown customer_id"


# ---------------------------------------------------------------------------
# 10. Business Rules
# ---------------------------------------------------------------------------

def test_product_prices_positive(generated_data):
    assert (generated_data["products"]["base_price"] > 0).all(), "Non-positive base_price found"


def test_order_item_quantities_positive(generated_data):
    assert (generated_data["order_items"]["quantity"] >= 1).all(), "quantity < 1 found"


def test_discount_does_not_exceed_unit_price(generated_data):
    df = generated_data["order_items"]
    violations = df[df["discount"] > df["unit_price"]]
    assert len(violations) == 0, f"{len(violations):,} rows where discount > unit_price"


def test_payment_amounts_non_negative(generated_data):
    assert (generated_data["payments"]["amount"] >= 0).all(), "Negative payment amounts found"


def test_valid_payment_statuses(generated_data, generator):
    actual   = set(generated_data["payments"]["payment_status"].unique())
    expected = set(generator.config["payment_statuses"])
    assert actual.issubset(expected), f"Unexpected payment statuses: {actual - expected}"


def test_valid_order_statuses(generated_data, generator):
    actual   = set(generated_data["orders"]["status"].unique())
    expected = set(generator.config["order_statuses"])
    assert actual.issubset(expected), f"Unexpected order statuses: {actual - expected}"


def test_valid_event_types(events_df, generator):
    actual   = set(events_df["event_type"].unique())
    expected = set(generator.config["event_types"])
    assert actual.issubset(expected), f"Unexpected event types: {actual - expected}"


# ---------------------------------------------------------------------------
# 11. Streaming — Problem D: Burst Traffic (compulsory)
# ---------------------------------------------------------------------------

def test_burst_peak_rate_exceeds_base(events_df, generator):
    """Peak events/min during burst windows must be >> base rate."""
    burst_multiplier = generator.config["burst_multiplier"]

    events_df = events_df.copy()
    events_df["minute"] = (
        events_df["event_timestamp"].dt.hour * 60
        + events_df["event_timestamp"].dt.minute
    )
    epm = events_df.groupby("minute").size()

    burst_minutes = set(range(12 * 60, 12 * 60 + 20)) | set(range(20 * 60, 20 * 60 + 20))
    peak_burst = epm[epm.index.isin(burst_minutes)].max()
    base_median = epm[~epm.index.isin(burst_minutes)].median()

    assert peak_burst >= base_median * (burst_multiplier / 2), (
        f"Burst peak {peak_burst:.0f} events/min is less than "
        f"half the expected {burst_multiplier}× base ({base_median * burst_multiplier / 2:.0f})"
    )


def test_base_rate_approximate(events_df, generator):
    """Median events/min outside burst windows ≈ base_events_per_min ±50%."""
    base_rate    = generator.config["base_events_per_min"]
    events_df    = events_df.copy()
    events_df["minute"] = (
        events_df["event_timestamp"].dt.hour * 60
        + events_df["event_timestamp"].dt.minute
    )
    epm = events_df.groupby("minute").size()
    burst_minutes = set(range(12 * 60, 12 * 60 + 20)) | set(range(20 * 60, 20 * 60 + 20))
    base_median = epm[~epm.index.isin(burst_minutes)].median()

    assert 0.5 * base_rate <= base_median <= 1.5 * base_rate, (
        f"Base median rate {base_median:.0f} events/min is outside 50% of "
        f"target {base_rate} events/min"
    )


# ---------------------------------------------------------------------------
# 12. Streaming — Problem E: Late Arrivals (compulsory)
# ---------------------------------------------------------------------------

def test_late_arrival_rate(events_df, generator):
    """~12% of all events (including duplicates) must have created_ts > event_timestamp ±3%.

    Measured without deduplication, matching the quality report's calculation.
    The generator pre-accounts for duplicates (which always appear late) so that
    this whole-population rate hits the configured target.
    """
    late_rate = (events_df["created_ts"] > events_df["event_timestamp"]).mean()
    target    = generator.config["late_arrival_rate"]
    assert abs(late_rate - target) <= 0.03, (
        f"Late arrival rate {late_rate:.1%} deviates from target {target:.0%} by more than 3%"
    )


def test_late_arrival_delay_within_range(events_df, generator):
    """Genuine late arrivals must be delayed within [late_delay_min, late_delay_max] minutes.

    Duplicate events (Problem F) also shift created_ts by 1-3 min, which would
    appear as sub-minimum delays. We check originals only (first occurrence of each
    event_id) to isolate true late arrivals from duplicate-injected shifts.
    """
    originals = events_df.drop_duplicates(subset=["event_id"], keep="first")
    late  = originals[originals["created_ts"] > originals["event_timestamp"]].copy()
    delay = (late["created_ts"] - late["event_timestamp"]).dt.total_seconds() / 60

    lo, hi = generator.config["late_delay_min_max"]
    assert (delay >= lo).all(), f"Some late events delayed less than {lo} min"
    assert (delay <= hi + 1).all(), f"Some late events delayed more than {hi} min"


def test_on_time_events_not_late(events_df):
    """Non-late events must have created_ts == event_timestamp (no unintended delay)."""
    clean = events_df.drop_duplicates(subset=["event_id"])
    on_time = clean[clean["created_ts"] <= clean["event_timestamp"]]
    # Allow tiny floating-point differences (sub-second)
    diff = (on_time["created_ts"] - on_time["event_timestamp"]).dt.total_seconds()
    assert (diff <= 0).all(), "On-time events have created_ts > event_timestamp unexpectedly"


# ---------------------------------------------------------------------------
# 13. Streaming — Problem F: Duplicate Event IDs (optional, chosen)
# ---------------------------------------------------------------------------

def test_stream_duplicate_rate(events_df, generator):
    """Quality-report duplicate rate (keep=False) must match config ±1%.

    The quality report uses duplicated(keep=False) which marks both the original
    and the re-emitted copy.  The generator injects stream_dup_rate/2 copies so
    this measurement hits the configured target.
    """
    total     = len(events_df)
    dup_count = events_df.duplicated(subset=["event_id"], keep=False).sum()
    rate      = dup_count / total
    target    = generator.config["duplicate_rate_stream"]
    assert abs(rate - target) <= 0.01, (
        f"Stream duplicate rate {rate:.2%} deviates from target {target:.2%} by more than 1%"
    )


def test_duplicate_events_have_same_event_type(events_df):
    """Re-emitted events must have the same event_type as the original."""
    dup_ids    = events_df[events_df.duplicated("event_id", keep=False)]["event_id"]
    dup_events = events_df[events_df["event_id"].isin(dup_ids)]
    grouped    = dup_events.groupby("event_id")["event_type"].nunique()
    mixed = grouped[grouped > 1]
    assert len(mixed) == 0, (
        f"{len(mixed)} event_ids have different event_type between original and duplicate"
    )


def test_duplicate_events_slight_ts_shift(events_df):
    """Re-emitted events must have created_ts shifted, not identical."""
    dup_ids    = events_df[events_df.duplicated("event_id", keep=False)]["event_id"]
    dup_events = events_df[events_df["event_id"].isin(dup_ids)].copy()
    grouped    = dup_events.groupby("event_id")["created_ts"]
    same_ts    = grouped.nunique()
    # All duplicates should have different created_ts
    all_same = (same_ts == 1).all()
    assert not all_same, "All duplicate events have identical created_ts — ts shift not applied"
