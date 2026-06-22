# 01_data_generator/generator.py
import yaml
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
import sys
from pathlib import Path
import json
import argparse
from dataclasses import dataclass

# Add project root to path so `config` is resolvable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import Settings
from config.logging import setup_logger

settings = Settings()


@dataclass
class GeneratedTable:
    df: pd.DataFrame
    filename: str

    def unpack(self):
        return self.df, self.filename


class DataGenerator:
    """Generates synthetic e-commerce data based on the provided configuration."""

    def __init__(
        self,
        config_path=settings.DATA_GENERATOR_CONFIG_PATH,
        output_path=settings.DATA_GENERATOR_OUTPUT_PATH,
    ):
        self.logger = setup_logger(name="DataGenerator", filename="DataGenerator.log")
        self.output_path = Path(output_path)
        self.logger.info(f"Output_path: {output_path}")
        self.config = self._load_config(config_path)
        self.logger.info(f"Config path loaded: {config_path}")

    def generate(self):
        customers = self._generate_customers(self.config)
        products = self._generate_products(self.config)
        orders = self._generate_orders(customers, self.config)
        order_items = self._generate_order_items(orders, products, self.config)
        payments = self._generate_payments(orders, order_items, self.config)
        for table in [customers, products, orders, order_items, payments]:
            self._save_as_csv(table)
        return customers, products, orders, order_items, payments

    def _to_df(self, data) -> pd.DataFrame:
        """Accept either a GeneratedTable or a plain DataFrame."""
        if isinstance(data, GeneratedTable):
            return data.df
        if isinstance(data, pd.DataFrame):
            return data
        raise TypeError(f"Expected GeneratedTable or DataFrame, got {type(data)}")

    def _save_as_csv(self, table: GeneratedTable):
        try:
            self.output_path.mkdir(parents=True, exist_ok=True)
            path = self.output_path / f"{table.filename}.csv"
            self.logger.info(f"Table: `{table.filename}` saved to path: {path}")
            table.df.to_csv(path, index=False)
        except FileNotFoundError as e:
            self.logger.error(f"File not found: {e}")

    def _parse_brand_config(self, cfg: dict) -> None:
        """Parse and validate cfg["brands"] into two derived lookup dicts.

        Adds to cfg in-place:
            cfg["_brand_names"]   : {category: [name, ...]}
            cfg["_brand_weights"] : {category: np.ndarray of normalised weights}

        Raises:
            ValueError: on missing categories, duplicate names, non-positive
                        weights, or categories not in product_categories.
        """
        known_categories = set(cfg["product_categories"])
        seen_categories = set()
        brand_names = {}
        brand_weights = {}

        for i, entry in enumerate(cfg["brands"]):
            # required keys present
            if "category" not in entry or "brands" not in entry:
                raise ValueError(
                    f"brands[{i}] is missing 'category' or 'brands' key: {entry}"
                )

            cat = entry["category"]
            brands = entry["brands"]

            # category is recognised
            if cat not in known_categories:
                raise ValueError(
                    f"brands[{i}]: category '{cat}' not in product_categories: "
                    f"{sorted(known_categories)}"
                )

            # no duplicate categories in the list
            if cat in seen_categories:
                raise ValueError(
                    f"brands[{i}]: category '{cat}' appears more than once"
                )
            seen_categories.add(cat)

            # each brand entry has name + weight
            for j, b in enumerate(brands):
                if "name" not in b or "weight" not in b:
                    raise ValueError(
                        f"brands[{i}].brands[{j}] is missing 'name' or 'weight': {b}"
                    )

            names = [b["name"] for b in brands]
            weights = [b["weight"] for b in brands]

            # no duplicate brand names within a category
            if len(names) != len(set(names)):
                dupes = [n for n in names if names.count(n) > 1]
                raise ValueError(
                    f"brands[{i}] ('{cat}'): duplicate brand names: {sorted(set(dupes))}"
                )

            # all weights are positive numbers
            for name, w in zip(names, weights):
                if not isinstance(w, (int, float)) or w <= 0:
                    raise ValueError(
                        f"brands[{i}] ('{cat}'): weight for '{name}' must be a "
                        f"positive number, got {w!r}"
                    )

            arr = np.array(weights, dtype=float)
            brand_names[cat] = names
            brand_weights[cat] = arr / arr.sum()  # normalise once, reuse everywhere

        # every product_category has a brands entry
        missing = known_categories - seen_categories
        if missing:
            raise ValueError(
                f"No brands entry for product_categories: {sorted(missing)}"
            )

        cfg["_brand_names"] = brand_names
        cfg["_brand_weights"] = brand_weights

    def _load_config(self, config_path) -> dict:
        try:
            with open(config_path, "r") as file:
                cfg = yaml.safe_load(file)
        except Exception as e:
            self.logger.error(f"Error loading config from {config_path}: {e}")
            raise

        cfg["sim_start"] = datetime.now() - timedelta(days=cfg["days_history"])
        cfg["sim_end"] = datetime.now()
        # Derive order ad order items
        cfg["n_orders"] = int(cfg["n_customers"] * cfg["avg_orders_per_customer"])
        cfg["n_order_items"] = int(cfg["n_orders"] * cfg["avg_items_per_order"])

        raw = cfg["schema_change_date"]
        if isinstance(raw, float):
            cfg["schema_change_date"] = cfg["sim_start"] + timedelta(
                seconds=raw * (cfg["sim_end"] - cfg["sim_start"]).total_seconds()
            )
        else:
            cfg["schema_change_date"] = datetime.strptime(raw, "%Y-%m-%d")
        try:
            self._parse_brand_config(cfg)
        except Exception as e:
            self.logger.error(f"Error parsing brand config: {e}")
            raise
        return cfg

    def _build_distribution_dirichlet(
        self, cfg, skew_dict: dict, alpha: float = 2.0
    ) -> dict:
        """Helper to build a distribution dict for a categorical variable with skewness."""
        rng = np.random.default_rng(cfg["random_seed"])

        fixed = {k: v for k, v in skew_dict.items() if isinstance(v, (int, float))}
        # self.logger.info(f"Fixed distribution: {fixed}")
        auto_keys = [k for k, v in skew_dict.items() if v == "auto"]
        # self.logger.info(f"Auto distribution keys: {auto_keys}")

        fixed_sum = sum(fixed.values())
        remaining = 1.0 - fixed_sum

        if remaining < 0:
            raise ValueError("Fixed probabilities exceed 1.0")

        raw = rng.dirichlet(np.ones(len(auto_keys)) * alpha)

        auto_dist = {k: w * remaining for k, w in zip(auto_keys, raw)}

        final = {**fixed, **auto_dist}
        # self.logger.info(f"Raw distribution: {final}")

        # normalize safety
        total = sum(final.values())
        final = {k: v / total for k, v in final.items()}
        # self.logger.info(f"Normalized distribution: {final}")
        return final

    def _sample_from_distribution(self, rng, dist: dict, size: int):
        """Helper to sample from a categorical distribution defined by dist dict."""
        labels = list(dist.keys())
        probs = np.array(list(dist.values()))
        probs = probs / probs.sum()
        # self.logger.debug(f"Sampling from distribution: {dist} with probs: {probs}")
        return rng.choice(labels, size=size, p=probs)

    def _generate_customers(self, cfg) -> GeneratedTable:
        """Generates a DataFrame of customers with the following schema:
        - customer_id: unique identifier (e.g., C000001)
        - signup_ts: timestamp of signup
        - country: skewed distribution with 85% "VN"
        - segment: skewed distribution with 60% bronze, 30% silver, 10% gold
        - marketing_opt_in: boolean with 70% opt-in rate
        """
        self.logger.info(f"[1/5] Generating {cfg['n_customers']:,} customers …")
        rng = np.random.default_rng(cfg["random_seed"])
        n = cfg["n_customers"]
        # segs = cfg["customer_segments"]

        # customer_id: C000001, C000002, ...
        customer_ids = np.char.add("C", np.char.zfill(np.arange(1, n + 1).astype("U10"), 6))
        # signup_ts: random timestamps within the last cfg["days_history"] days
        sigup_deltas = rng.integers(0, cfg["days_history"] * 24 * 60 * 60, size=n)
        signup_ts = pd.Timestamp(cfg["sim_start"]) + pd.to_timedelta(sigup_deltas, unit="s")
        # segment: skewed distribution with 60% bronze, 30% silver, 10% gold
        seg_weights = self._build_distribution_dirichlet(
            cfg, cfg["customer_segment_distribution"], alpha=2.0
        )
        segments = self._sample_from_distribution(rng, seg_weights, size=n)
        # country: skewed distribution with 85% "VN"
        country_dist = self._build_distribution_dirichlet(
            cfg, cfg["countries_distribution"], alpha=2.0
        )
        countries = self._sample_from_distribution(rng, country_dist, size=n)

        # marketing_opt_in: 70% opt-in rate
        opt_in = rng.random(n) < cfg["marketing_opt_in_rate"]

        df = pd.DataFrame(
            {
                "customer_id": customer_ids,
                "signup_ts": signup_ts,
                "country": countries,
                "segment": segments,
                "marketing_opt_in": opt_in,
            }
        )
        filename = f"customer_{n}"
        return GeneratedTable(df=df, filename=filename)

    def _generate_products(self, cfg) -> GeneratedTable:
        """Generates a DataFrame of products with the following schema:
        - product_id: unique identifier (e.g., P000001)
        - category: one of the categories defined in cfg["product_categories"]
        - brand: one of the brands defined in cfg["brands"]
        - base_price: random price between cfg["price_range"][0] and cfg["price_range"][1]
        - is_active: boolean with 90% active rate
        - created_ts: random timestamp within the last cfg["days_history"] days
        """
        # schema_change: if product_id < cfg.schema_change_product_id:
        #   category = None, price = None
        self.logger.info(f"[2/5] Generating {cfg['n_products']:,} products …")
        rng = np.random.default_rng(cfg["random_seed"] + 1)
        n = cfg["n_products"]
        # cats = cfg["product_categories"]
        # brands = cfg["brands"]

        # product_id: P000001, P000002, ...
        product_ids = np.char.add("P", np.char.zfill(np.arange(1, n + 1).astype("U10"), 6))
        # category: random choice from cfg["product_categories"]
        category_dist = self._build_distribution_dirichlet(
            cfg, cfg["category_distribution"], alpha=2.0
        )

        categories = self._sample_from_distribution(rng, category_dist, size=n)
        # brand: random choice from cfg["brands"], vectorized per category
        brands = np.empty(n, dtype=object)
        for cat in cfg["product_categories"]:
            mask = categories == cat
            count = int(mask.sum())
            if count > 0:
                brands[mask] = rng.choice(
                    cfg["_brand_names"][cat], size=count, p=cfg["_brand_weights"][cat]
                )
        # base_price — vectorized per category
        self.logger.info(f"Price range: {cfg['price_range']}")
        price_cfg = cfg["price_range"]
        prices = np.empty(n)
        for cat in cfg["product_categories"]:
            lo, hi = price_cfg[cat]
            mask = categories == cat
            count = int(mask.sum())
            if count > 0:
                prices[mask] = np.round(rng.uniform(lo, hi, size=count), 2)
        # is_active
        is_active = rng.random(n) < 0.9
        # created_ts
        created_deltas = rng.integers(0, cfg["days_history"] * 24 * 60 * 60, size=n)
        created_ts = pd.Timestamp(cfg["sim_start"]) + pd.to_timedelta(created_deltas, unit="s")
        df = pd.DataFrame(
            {
                "product_id": product_ids,
                "category": categories,
                "brand": brands,
                "base_price": prices,
                "is_active": is_active,
                "created_ts": created_ts,
            }
        )
        filename = f"products_{n}"
        return GeneratedTable(df=df, filename=filename)

    def _generate_orders(self, customers, cfg) -> GeneratedTable:
        """Generates a DataFrame of orders with the following schema:
        - order_id: unique identifier (e.g., O000001)
        - customer_id: foreign key to customers
        - order_timestamp: random timestamp between customer's signup_ts and now
        - status: one of field in cfg["order_status"] with probabilities defined in cfg["order_status_distribution"]
        - shipping_city: same as customer's country 90% of the time, otherwise random from cfg["cities"]
        - shipping_method: one of field in cfg["shipping_methods"] with probabilities defined in cfg["shipping_method_distribution"]
        - coupon_code: 20% of orders have a coupon code (e.g., SAVE20)
        """
        # schema_change: if order_timestamp < cfg.schema_change_date:
        #   coupon_code = None, shipping_method = None
        customers_df = self._to_df(customers)
        self.logger.info(f"[3/5] Generating {cfg['n_orders']:,} orders …")
        rng = np.random.default_rng(cfg["random_seed"] + 2)
        n = cfg["n_orders"]
        # order_ids: O000001, O000002, ...
        order_ids = np.char.add("O", np.char.zfill(np.arange(1, n + 1).astype("U10"), 6))
        # customer_id: random choice from customers.customer_id
        customer_ids = customers_df["customer_id"].values
        chosen_customers = rng.choice(customer_ids, size=n)
        # order_timestamp: vectorized — uniform fraction of each customer's available window
        signup_series = customers_df.set_index("customer_id")["signup_ts"]
        chosen_signup = pd.to_datetime(signup_series.reindex(chosen_customers).values)
        sim_end_ts = pd.Timestamp(cfg["sim_end"])
        gaps_s = np.maximum(1, (sim_end_ts - chosen_signup).total_seconds().astype(np.int64))
        order_ts = chosen_signup + pd.to_timedelta((rng.random(n) * gaps_s).astype(np.int64), unit="s")
        # status
        status_dist = self._build_distribution_dirichlet(
            cfg, cfg["order_status_distribution"], alpha=2.0
        )
        statuses = self._sample_from_distribution(rng, status_dist, size=n)
        # shipping_city — sample directly from cities_distribution so HCMC hits the
        # configured skew_ratio_city target exactly (Problem A).
        city_dist = self._build_distribution_dirichlet(
            cfg, cfg["cities_distribution"], alpha=2.0
        )
        shipping_cities = self._sample_from_distribution(rng, city_dist, size=n)
        # shipping_method
        shipping_dist = self._build_distribution_dirichlet(
            cfg, cfg["shipping_method_distribution"], alpha=2.0
        )
        shipping_methods = self._sample_from_distribution(rng, shipping_dist, size=n)
        # coupon_code
        coupon_code = np.where(rng.random(n) < 0.2, "SAVEUPTO20", None)

        df = pd.DataFrame(
            {
                "order_id": order_ids,
                "customer_id": chosen_customers,
                "order_timestamp": order_ts,
                "status": statuses,
                "shipping_city": shipping_cities,
                "shipping_method": shipping_methods,
                "coupon_code": coupon_code,
            }
        )
        # Add missing 60% shipping methods and coupon code from old timeline
        df["order_timestamp"] = pd.to_datetime(df["order_timestamp"])
        old_mask = df["order_timestamp"] < pd.Timestamp(cfg["schema_change_date"])

        df.loc[old_mask, "coupon_code"] = None
        df.loc[old_mask, "shipping_method"] = None
        filename = f"orders_{n}"
        return GeneratedTable(df=df, filename=filename)

    def _generate_order_items(self, orders, products, cfg) -> GeneratedTable:
        """Generates a DataFrame of order items with the following schema:
        - order_item_id: unique identifier (e.g., OI000001)
        - order_id: foreign key to orders
        - product_id: foreign key to products
        - quantity: random integer between 1 and 10
        - unit_price: product's base_price at the time of order (considering price changes)
        - discount_amount: random discount between 0 and 30% of unit_price
        """
        orders_df = self._to_df(orders)
        products_df = self._to_df(products)

        self.logger.info(f"[4/5] Generating {cfg['n_order_items']:,} order_items …")
        rng = np.random.default_rng(cfg["random_seed"] + 3)
        n = cfg["n_order_items"]

        # order_item_ids: OI000000001, OI000000002, ...
        order_item_ids = np.char.add("OI", np.char.zfill(np.arange(1, n + 1).astype("U12"), 9))
        # Sample order_ids from exisiting orders
        order_ids = rng.choice(orders_df["order_id"].values, size=n)
        # Sample product_ids from existing products
        product_ids = rng.choice(products_df["product_id"].values, size=n)
        quantities = rng.integers(1, 11, size=n)
        # Look up base_price for each sampled product — vectorized via pandas reindex
        unit_prices = products_df.set_index("product_id")["base_price"].reindex(product_ids).values
        # Discount: 0–30% of unit_price
        discount_amount = np.round(unit_prices * rng.uniform(0.0, 0.3, size=n), 2)

        df = pd.DataFrame(
            {
                "order_item_id": order_item_ids,
                "order_id": order_ids,
                "product_id": product_ids,
                "quantity": quantities,
                "unit_price": unit_prices,
                "discount": discount_amount,
            }
        )
        # Inject dup_rate/2 rows so the quality report's keep=False measurement
        # (which marks both original and copy) reads back ≈ duplicate_rate_offline.
        dup_mask = df.sample(frac=cfg["duplicate_rate_offline"] / 2, random_state=42)
        df = pd.concat([df, dup_mask], ignore_index=True)
        filename = f"order_items_{n}"
        return GeneratedTable(df=df, filename=filename)

    def _generate_payments(self, orders, order_items, cfg) -> GeneratedTable:
        """Generates a DataFrame of payments with the following schema:
        - payment_id: unique identifier (e.g., P000001)
        - order_id: foreign key to orders
        - payment_timestamp: random timestamp between order_timestamp and now
        - amount: the payment amount (should match the total amount of the
            order)
        - payment_method: one in cfg["payment_methods"] with
            probabilities defined in cfg["payment_method_distribution"]
        - payment_status: one of field in cfg["payment_statuses"] with
            probabilities defined in cfg["payment_status_distribution"]
        """
        orders_df = self._to_df(orders)
        order_items_df = self._to_df(order_items)
        self.logger.info("[5/5] Generating payments …")
        rng = np.random.default_rng(cfg["random_seed"] + 4)
        n = len(orders_df)

        payment_ids = np.char.add("PAY", np.char.zfill(np.arange(1, n + 1).astype("U10"), 6))

        # One payment per order
        order_ids = orders_df["order_id"].values

        # Payment timestamp: vectorized — uniform fraction of window between order_ts and sim_end
        order_ts = pd.to_datetime(orders_df["order_timestamp"].values)
        gaps_s = np.maximum(1, (pd.Timestamp(cfg["sim_end"]) - order_ts).total_seconds().astype(np.int64))
        payment_ts = order_ts + pd.to_timedelta((rng.random(n) * gaps_s).astype(np.int64), unit="s")

        # Amount
        order_items_df["line_total"] = (
            order_items_df["unit_price"] * order_items_df["quantity"]
        ) - order_items_df["discount"]

        order_totals = order_items_df.groupby("order_id")["line_total"].sum().round(2)

        # Map totals back to the orders; default to 0.0 if an order has no items
        amounts = orders_df["order_id"].map(order_totals).fillna(0.0).values

        payment_method_dist = self._build_distribution_dirichlet(
            cfg, cfg["payment_method_distribution"], alpha=2.0
        )
        payment_methods = self._sample_from_distribution(
            rng, payment_method_dist, size=n
        )

        payment_status_dist = self._build_distribution_dirichlet(
            cfg, cfg["payment_status_distribution"], alpha=2.0
        )
        payment_statuses = self._sample_from_distribution(
            rng, payment_status_dist, size=n
        )

        df = pd.DataFrame(
            {
                "payment_id": payment_ids,
                "order_id": order_ids,
                "payment_timestamp": payment_ts,
                "amount": amounts,
                "payment_method": payment_methods,
                "payment_status": payment_statuses,
            }
        )
        filename = f"payments_{n}"
        return GeneratedTable(df=df, filename=filename)

    # ---------- STREAMING EVENTS -----------
    """ (24-hour simulation)
    Problems injected:
    (d) Burst traffic at 12:00-12:20 and 20:00-20:20  (compulsory)
    (e) 12 % late arrivals   created_ts > event_timestamp  (compulsory)
    (f) 1.5 % duplicate event_ids                          (optional)
    """

    def _parse_burst_windows(self):
        """windows: list[str]) -> list[tuple[int, int]"""

        windows = self.config["burst_windows"]
        result = []
        for w in windows:
            start_s, end_s = w.split("-")
            sh, sm = map(int, start_s.split(":"))
            eh, em = map(int, end_s.split(":"))
            result.append((sh * 60 + sm, eh * 60 + em))
        return result

    def generate_stream_events(
        self,
        customers_df: pd.DataFrame,
        products_df: pd.DataFrame,
    ) -> list[dict]:
        """
        Simulates one full day of e-commerce clickstream events.

        DATA PROBLEM D – burst traffic (compulsory):
            Base rate = 100 events/min.
            During burst windows (12:00-12:20, 20:00-20:20):
                rate = 100 × 30 = 3 000 events/min.
            Downstream streaming pipelines must handle backpressure;
            Flink watermarks must tolerate the event-time gap.

        DATA PROBLEM E – late arrivals (compulsory):
            12 % of events have created_ts delayed by 5–45 minutes
            after event_timestamp.  Flink's AllowedLateness /
            WatermarkStrategy must be configured to handle these.

        DATA PROBLEM F – duplicate event_ids (optional, chosen):
            1.5 % of event_ids are re-emitted (same id, slight ts shift).
            Stream dedup must key on event_id + event_timestamp.
        """
        print("[stream] Simulating 24-hour event stream …")
        rng = np.random.default_rng(self.config["random_seed"] + 5)

        customer_ids = customers_df["customer_id"].values
        product_ids = products_df["product_id"].values
        burst_windows = self._parse_burst_windows()
        sim_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # Per-minute rates (Problem D): build once, sample all minutes at once
        rates = np.full(24 * 60, float(self.config["base_events_per_min"]))
        for start_m, end_m in burst_windows:
            rates[start_m:end_m] *= self.config["burst_multiplier"]

        n_per_minute = rng.poisson(rates)
        total = int(n_per_minute.sum())
        minute_idx = np.repeat(np.arange(24 * 60), n_per_minute)

        # Timestamps — one vectorized call per column
        sim_ts = pd.Timestamp(sim_date)
        ev_offsets_s = minute_idx * 60.0 + rng.uniform(0, 60, size=total)
        ev_timestamps = sim_ts + pd.to_timedelta(ev_offsets_s, unit="s")

        # Event types — single rng.choice for all events
        ev_dist = self._build_distribution_dirichlet(
            self.config, self.config["event_type_distribution"], alpha=2.0
        )
        ev_type_labels = np.array(list(ev_dist.keys()))
        ev_type_probs = np.array(list(ev_dist.values()), dtype=float)
        ev_type_probs /= ev_type_probs.sum()
        ev_types = rng.choice(ev_type_labels, size=total, p=ev_type_probs)

        # Customer / product IDs — single rng.choice each
        chosen_customers = rng.choice(customer_ids, size=total).astype(str)
        chosen_products = rng.choice(product_ids, size=total).astype(str)

        # Session IDs
        session_nums = rng.integers(1, 999_999, size=total)
        session_ids = np.char.add("S", np.char.zfill(session_nums.astype("U10"), 8))

        # Event IDs
        event_ids = np.char.add("E", np.char.zfill(np.arange(1, total + 1).astype("U15"), 12))

        # Conditional columns: generate full arrays, null out non-applicable rows
        qty_mask = np.isin(ev_types, ["purchase", "add_to_cart"])
        quantities = rng.integers(1, 4, size=total).astype(object)
        quantities[~qty_mask] = None

        price_mask = ev_types == "purchase"
        prices = np.round(rng.uniform(10, 500, size=total), 2).astype(object)
        prices[~price_mask] = None

        # LATE ARRIVAL injection (E) — vectorized nanosecond arithmetic
        n_dups = int(total * self.config["duplicate_rate_stream"] / 2)
        total_with_dups = total + n_dups
        n_late = max(0, int(total_with_dups * self.config["late_arrival_rate"]) - n_dups)
        late_min, late_max = self.config["late_delay_min_max"]
        late_indices = rng.choice(total, size=n_late, replace=False)
        late_delays_ns = rng.integers(late_min, late_max + 1, size=n_late).astype(np.int64) * 60 * 1_000_000_000
        created_ns = ev_timestamps.asi8.copy()
        created_ns[late_indices] += late_delays_ns
        created_timestamps = pd.to_datetime(created_ns)

        # DUPLICATE injection (F) — slice + shift created_ts
        dup_indices = rng.choice(total, size=n_dups, replace=False)
        shift_ns = rng.integers(1, 4, size=n_dups).astype(np.int64) * 60 * 1_000_000_000

        # Stringify timestamps (vectorized strftime, not per-event isoformat loop)
        fmt = "%Y-%m-%dT%H:%M:%S.%f"
        ev_ts_str = ev_timestamps.strftime(fmt)
        cr_ts_str = created_timestamps.strftime(fmt)

        df = pd.DataFrame({
            "event_id":        event_ids,
            "event_type":      ev_types,
            "event_timestamp": ev_ts_str,
            "created_ts":      cr_ts_str,
            "customer_id":     chosen_customers,
            "session_id":      session_ids,
            "product_id":      chosen_products,
            "order_id":        None,
            "quantity":        quantities,
            "price":           prices,
        })

        # Append duplicate rows with shifted created_ts
        dup_df = df.iloc[dup_indices].copy()
        dup_created_ns = created_ns[dup_indices] + shift_ns
        dup_df["created_ts"] = pd.to_datetime(dup_created_ns).strftime(fmt)

        all_df = pd.concat([df, dup_df], ignore_index=True)
        all_df.sort_values("created_ts", inplace=True, ignore_index=True)

        total_events = len(all_df)
        n_late_total = n_late + n_dups
        print(
            f"    → {total_events:,} events  |  "
            f"late={n_late_total:,} ({n_late_total / total_events:.1%})  |  "
            f"duplicates (keep=False)={n_dups * 2:,} ({n_dups * 2 / total_events:.1%})"
        )
        return all_df.to_dict("records")

    # --------------- QUALITY REPORT ----------------------

    def quality_report(
        self,
        customers_df: pd.DataFrame,
        products_df: pd.DataFrame,
        orders_df: pd.DataFrame,
        order_items_df: pd.DataFrame,
        payments_df: pd.DataFrame,
        events: list[dict],
    ) -> str:
        """
        Produce a human-readable quality report that is committed as evidence.
        """
        lines = [
            "=" * 70,
            "FSDS E-COMMERCE DATA GENERATOR — QUALITY REPORT",
            f"Generated: {datetime.now().isoformat()} UTC",
            f"Random seed: {self.config['random_seed']}",
            "=" * 70,
            "",
            "OFFLINE TABLES---",
            "",
            "Table row counts:",
            f"  customers  : {len(customers_df):>10,}",
            f"  products   : {len(products_df):>10,}",
            f"  orders     : {len(orders_df):>10,}",
            f"  order_items: {len(order_items_df):>10,}  (includes injected dups)",
            f"  payments   : {len(payments_df):>10,}",
            "",
        ]

        # Cardinality
        lines += [
            "High-cardinality columns (approx unique):",
            f"  customer_id unique : {customers_df['customer_id'].nunique():,}",
            f"  product_id  unique : {products_df['product_id'].nunique():,}",
            f"  order_id    unique : {orders_df['order_id'].nunique():,}",
            "",
        ]

        # PROBLEM A: City skew
        city_dist = orders_df["shipping_city"].value_counts(normalize=True)
        lines += [
            "PROBLEM A — Geographic skew in orders.shipping_city:",
            f"  Target  : HCMC = {self.config['skew_ratio_city']:.0%}",
            f"  Actual  : HCMC = {city_dist.iloc[0]:.1%}",
        ]
        for city, pct in city_dist.items():
            lines.append(f"    {str(city):<25}: {pct:.1%}")
        lines.append("")

        # PROBLEM B: Schema evolution
        change_date = self.config["schema_change_date"]
        old_orders = orders_df[
            orders_df["order_timestamp"] < change_date.date().isoformat()
        ]
        new_orders = orders_df[
            orders_df["order_timestamp"] >= change_date.date().isoformat()
        ]
        lines += [
            "PROBLEM B — Schema evolution (orders before schema_change_date):",
            f"  schema_change_date : {change_date.date()}",
            f"  Old orders (<date) : {len(old_orders):,} rows",
            f"    coupon_code NULL    : {old_orders['coupon_code'].isna().mean():.0%}  ← expected 100%",
            f"    shipping_method NULL: {old_orders['shipping_method'].isna().mean():.0%}  ← expected 100%",
            f"  New orders (>=date): {len(new_orders):,} rows",
            f"    coupon_code NULL    : {new_orders['coupon_code'].isna().mean():.1%}  ← ~75% (no coupon)",
            f"    shipping_method NULL: {new_orders['shipping_method'].isna().mean():.0%}  ← expected 0%",
            "",
        ]

        # PROBLEM C: Duplicate order_items
        natural_key = ["order_id", "product_id", "unit_price"]
        total = len(order_items_df)
        dup_mask = order_items_df.duplicated(subset=natural_key, keep=False)
        n_dup_rows = dup_mask.sum()
        after_dedup = order_items_df.drop_duplicates(subset=natural_key)
        lines += [
            "PROBLEM C — Duplicate rows in order_items:",
            f"  Natural key         : {natural_key}",
            f"  Total rows          : {total:,}",
            f"  Duplicate rows      : {n_dup_rows:,}  ({n_dup_rows/total:.1%})",
            f"  Target dup rate     : {self.config['duplicate_rate_offline']:.1%}",
            f"  After dedup         : {len(after_dedup):,} rows",
            "",
        ]

        # Category skew
        cat_dist = products_df["category"].value_counts(normalize=True)
        lines += [
            "Category skew in products:",
            f"  Target  : electronics = {self.config['skew_ratio_category']:.0%}",
            f"  Actual  : electronics = {cat_dist.get('electronics', 0):.1%}",
        ]
        for cat, pct in cat_dist.items():
            lines.append(f"    {str(cat):<20}: {pct:.1%}")
        lines.append("")

        # Payment failure rate
        fail_rate = payments_df["payment_status"].eq("failed").mean()
        lines += [
            "Payment stats:",
            f"  Failure rate        : {fail_rate:.1%}",
            # f"  Retry rows          : {payments_df['attempt_number'].gt(1).sum():,}",
            "",
        ]

        # STREAMING
        lines.append("STREAMING EVENTS ---")
        lines.append("")

        if not events:
            lines.append(
                "  (stream skipped — run without --skip-stream to generate events)"
            )
        else:
            events_df = pd.DataFrame(events)
            total_events = len(events_df)

            late_mask = events_df["created_ts"] > events_df["event_timestamp"]
            n_late = late_mask.sum()

            # Duplicate event_ids (same id = reemit)
            n_dup_ev = events_df.duplicated(subset=["event_id"], keep=False).sum()

            # Burst: events per minute
            events_df["minute"] = (
                pd.to_datetime(events_df["event_timestamp"]).dt.hour * 60
                + pd.to_datetime(events_df["event_timestamp"]).dt.minute
            )
            epm = events_df.groupby("minute").size()
            peak_minute = int(epm.idxmax())
            peak_rate = int(epm.max())
            median_rate = int(epm.median())

            lines += [
                f"Total events        : {total_events:,}",
                "",
                "PROBLEM D — Burst traffic:",
                f"  Base rate          : {self.config['base_events_per_min']} events/min",
                f"  Burst multiplier   : {self.config['burst_multiplier']}×",
                f"  Burst windows      : {self.config['burst_windows']}",
                f"  Median rate        : {median_rate} events/min",
                f"  Peak rate          : {peak_rate:,} events/min (minute {peak_minute})",
                "",
                "PROBLEM E — Late arrivals:",
                f"  Target rate        : {self.config['late_arrival_rate']:.0%}",
                f"  Actual late rows   : {n_late:,} ({n_late/total_events:.1%})",
                f"  Delay range        : {self.config['late_delay_min_max']} minutes",
                "",
                "PROBLEM F — Duplicate event_ids:",
                f"  Target rate        : {self.config['duplicate_rate_stream']:.1%}",
                f"  Duplicate rows     : {n_dup_ev:,} ({n_dup_ev/total_events:.1%})",
                "",
                "Event type distribution:",
            ]
            for ev_type, cnt in events_df["event_type"].value_counts().items():
                lines.append(f"  {str(ev_type):<20}: {cnt:,}  ({cnt/total_events:.1%})")

        lines += [
            "",
            "=" * 70,
            "END OF REPORT",
            "=" * 70,
        ]

        report = "\n".join(lines)
        report_path = self.output_path / "quality_report.txt"
        report_path.write_text(report, encoding="utf-8")
        return report

    # 10.  Write outputs

    def write_outputs(
        self,
        customers_df: pd.DataFrame,
        products_df: pd.DataFrame,
        orders_df: pd.DataFrame,
        order_items_df: pd.DataFrame,
        payments_df: pd.DataFrame,
        events: list[dict],
    ):
        # path = Path(self.output_path) / f"{table.filename}.csv"
        offline_dir = self.output_path / "offline"
        streaming_dir = self.output_path / "streaming"
        offline_dir.mkdir(parents=True, exist_ok=True)
        streaming_dir.mkdir(parents=True, exist_ok=True)

        print("\n[write] Saving outputs …")

        # Parquet (with pyarrow if available, else CSV fallback)
        try:
            import pyarrow  # noqa: F401

            engine = "pyarrow"
        except ImportError:
            engine = "fastparquet" if self._has_fastparquet() else None

        tables = {
            "customers": customers_df,
            "products": products_df,
            "orders": orders_df,
            "order_items": order_items_df,
            "payments": payments_df,
        }

        for name, df in tables.items():
            if engine:
                path = offline_dir / f"{name}.parquet"
                df.to_parquet(path, engine=engine, index=False)
            else:
                # Fallback: CSV (works everywhere; student converts locally)
                path = offline_dir / f"{name}.csv"
                df.to_csv(path, index=False)
                print(f"    [WARN] pyarrow not found — wrote {name}.csv instead.")
            print(f"    ✓ {name:<15}: {len(df):>9,} rows → {path}")

        # Streaming JSON (newline-delimited)
        events_path = streaming_dir / "events.json"
        with open(events_path, "w") as fh:
            for ev in events:
                fh.write(json.dumps(ev) + "\n")
        print(f"    ✓ events         : {len(events):>9,} events → {events_path}")

    def _has_fastparquet(self) -> bool:
        try:
            import fastparquet  # noqa: F401

            return True
        except ImportError:
            return False


def main():
    generator = DataGenerator(config_path=settings.DATA_GENERATOR_CONFIG_PATH)
    customers, products, orders, order_items, payments = generator.generate()

    parser = argparse.ArgumentParser(
        description="FSDS E-Commerce Data Generator"
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent / "config.yaml"),
        help="Path to config.yaml",
    )
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).parent / "outputs"),
        help="Directory for generated files",
    )
    parser.add_argument(
        "--skip-stream",
        action="store_true",
        help="Skip streaming event generation (faster dev loop)",
    )
    args = parser.parse_args()
    if args.skip_stream:
        events = []
        print("[stream] Skipped (--skip-stream flag)")
    else:
        events = generator.generate_stream_events(customers.df, products.df)

    generator.write_outputs(
        customers.df, products.df, orders.df, order_items.df, payments.df, events
    )

    print("\n[report] Writing quality report …")
    report = generator.quality_report(
        customers.df, products.df, orders.df, order_items.df, payments.df, events
    )
    print("\n" + report)
    print(f"\n[done] All outputs in: {generator.output_path.resolve()}")


if __name__ == "__main__":
    main()
