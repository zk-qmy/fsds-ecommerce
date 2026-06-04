# 01_data_generator/generator.py
import yaml
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
from config.settings import Settings
from config.logging import setup_logger
settings = Settings()


class DataGenerator:
    """Generates synthetic e-commerce data based on the provided configuration."""

    def __init__(self, config_path=settings.DATA_GENERATOR_CONFIG_PATH):
        self.logger = setup_logger(name="DataGenerator")
        self.config = self._load_config(config_path)
        self.logger.info(f"Config path loaded: {config_path}")

    def generate(self):
        customers = self._generate_customers(self.config)
        products = self._generate_products(self.config)
        orders = self._generate_orders(customers, self.config)
        order_items = self._generate_order_items(orders, products, self.config)
        payments = self._generate_payments(orders, self.config)

        return customers, products, orders, order_items, payments

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

        cfg["schema_change_date"] = datetime.strptime(
            cfg["schema_change_date"], "%Y-%m-%d"
        )
        cfg["sim_start"] = datetime.now() - timedelta(days=cfg["days_history"])
        cfg["sim_end"] = datetime.now()
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
        self.logger.info(f"Fixed distribution: {fixed}")
        auto_keys = [k for k, v in skew_dict.items() if v == "auto"]
        self.logger.info(f"Auto distribution keys: {auto_keys}")

        fixed_sum = sum(fixed.values())
        remaining = 1.0 - fixed_sum

        if remaining < 0:
            raise ValueError("Fixed probabilities exceed 1.0")

        raw = rng.dirichlet(np.ones(len(auto_keys)) * alpha)

        auto_dist = {k: w * remaining for k, w in zip(auto_keys, raw)}

        final = {**fixed, **auto_dist}
        self.logger.info(f"Raw distribution: {final}")

        # normalize safety
        total = sum(final.values())
        final = {k: v / total for k, v in final.items()}
        self.logger.info(f"Normalized distribution: {final}")
        return final

    def _sample_from_distribution(self, rng, dist: dict, size: int):
        """Helper to sample from a categorical distribution defined by dist dict."""
        labels = list(dist.keys())
        probs = np.array(list(dist.values()))
        probs = probs / probs.sum()
        self.logger.info(f"Sampling from distribution: {dist} with probs: {probs}")
        return rng.choice(labels, size=size, p=probs)

    def _generate_customers(self, cfg) -> pd.DataFrame:
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
        customer_ids = [f"C{str(i).zfill(6)}" for i in range(1, n + 1)]
        # signup_ts: random timestamps within the last cfg["days_history"] days
        sigup_deltas = rng.integers(0, cfg["days_history"] * 24 * 60 * 60, size=n)
        signup_ts = [cfg["sim_start"] + timedelta(seconds=int(s)) for s in sigup_deltas]
        # segment: skewed distribution with 60% bronze, 30% silver, 10% gold
        seg_weights = self._build_distribution_dirichlet(
            cfg, cfg["customer_segment_distribution"], alpha=2.0
        )
        segments = self._sample_from_distribution(rng, seg_weights, size=n)
        # country: skewed distribution with 85% "VN"
        country_dist = self._build_distribution_dirichlet(
            cfg, cfg["country_distribution"], alpha=2.0
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
        return df

    def _generate_products(self, cfg) -> pd.DataFrame:
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
        product_ids = [f"P{str(i).zfill(6)}" for i in range(1, n + 1)]
        # category: random choice from cfg["product_categories"]
        category_dist = self._build_distribution_dirichlet(
            cfg, cfg["category_distribution"], alpha=2.0
        )

        categories = self._sample_from_distribution(rng, category_dist, size=n)
        # brand: random choice from cfg["brands"]
        brands = [
            rng.choice(
                cfg["_brand_names"][cat],
                p=cfg["_brand_weights"][cat],
            )
            for cat in categories
        ]
        # base_price
        self.logger.info(f"Price range: {cfg['price_range']}")
        price_cfg = cfg["price_range"]
        prices = [
            round(rng.uniform(price_cfg[cat][0], price_cfg[cat][1]), 2)
            for cat in categories
        ]
        # is_active
        is_active = rng.random(n) < 0.9
        # created_ts
        created_deltas = rng.integers(0, cfg["days_history"] * 24 * 60 * 60, size=n)
        created_ts = [
            cfg["sim_start"] + timedelta(seconds=int(s)) for s in created_deltas
        ]
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
        return df

    def _generate_orders(self, customers, cfg) -> pd.DataFrame:
        """Generates a DataFrame of orders with the following schema:
        - order_id: unique identifier (e.g., O000001)
        - customer_id: foreign key to customers
        - order_timestamp: random timestamp between customer's signup_ts and now
        - status: one of field in cfg["order_status"] with probabilities defined in cfg["order_status_distribution"]
        - shipping_city: same as customer's country 90% of the time, otherwise random from cfg["cities"]
        - shipping_method: one of field in cfg["shipping_methods"] with probabilities defined in cfg["shipping_method_distribution"]
        - coupon_code: 20% of orders have a coupon code (e.g., SAVE20)
        """
        # schema_change: if order_date < cfg.schema_change_date:
        #   coupon_code = None, shipping_method = None
        pass

    def _generate_order_items(self, orders, products, cfg):
        """Generates a DataFrame of order items with the following schema:
        - order_item_id: unique identifier (e.g., OI000001)
        - order_id: foreign key to orders
        - product_id: foreign key to products
        - quantity: random integer between 1 and 10
        - unit_price: product's base_price at the time of order (considering price changes)
        - discount_amount: random discount between 0 and 30% of unit_price
        """
        # after generation: inject 2% duplicates
        # dup_mask = df.sample(frac=0.02, random_state=42)
        # df = pd.concat([df, dup_mask]).sort_index()
        pass

    def _generate_payments(self, orders, cfg):
        """Generates a DataFrame of payments with the following schema:
        - payment_id: unique identifier (e.g., P000001)
        - order_id: foreign key to orders
        - payment_timestamp: random timestamp between order_timestamp and now
        - amount: the payment amount (should match the total amount of the order)
        - payment_method: one in cfg["payment_methods"] with probabilities defined in cfg["payment_method_distribution"]
        - payment_status: one of field in cfg["payment_statuses"] with probabilities defined in cfg["payment_status_distribution"]
        """
        pass
