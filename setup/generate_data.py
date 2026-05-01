"""
E-Commerce Lakehouse Dataset Generator
=======================================
Generates synthetic landing files for the AWS lakehouse pipeline.

Output matches the S3 key structure expected by DAG 1 and DAG 2:
    landing/<data_interval_start>/<table>/<file>

Run for a specific interval:
    python generate_ecommerce_data.py --date 2024-01-15

Run with schema drift (simulates a source system update):
    python generate_ecommerce_data.py --date 2024-01-02 --schema-drift

Run for today:
    python generate_ecommerce_data.py

Files generated (landing files only — staging/transactions are pipeline responsibilities):
    landing/<date>/customers/customers.csv
    landing/<date>/products/products.csv
    landing/<date>/orders/orders.json    ← newline-delimited JSON
    landing/<date>/order_items/order_items.csv

Defects injected into orders (so the pipeline validation gates have something to catch):
    V1  customer_id references a non-existent customer
    V2  product_id in order_items references a non-existent product
    V3  unit_price in order_items doesn't match the catalogue price
    V5  declared_total doesn't match the sum of line items
"""

import argparse
import json
import os
import random
import re
import uuid
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# ── Args ────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument(
    "--date",
    default=date.today().isoformat(),
    help="Data interval start date (YYYY-MM-DD). Defaults to today.",
)
parser.add_argument(
    "--schema-drift",
    action="store_true",
    default=False,
    help=(
        "Introduce new columns into landing files to simulate a source system update. "
        "orders gains: delivery_window (string), gift_message (boolean). "
        "customers gains: preferred_contact_method (string). "
        "Use this flag on a second run after a base interval to demonstrate "
        "how DAG 2 silently drops new columns while DAG 4 propagates them."
    ),
)
args = parser.parse_args()
schema_drift = args.schema_drift

try:
    interval_start = date.fromisoformat(args.date)
except ValueError:
    raise ValueError(f"--date must be YYYY-MM-DD, got: '{args.date}'")

interval_start_str = interval_start.isoformat()

fake = Faker()

# ── Volume ──────────────────────────────────────────────────────────────────────
N_CUSTOMERS = 500
N_PRODUCTS  = 120
N_ORDERS    = 1_200

# Defect injection rates
DEFECT_RATES = {
    "bad_customer" : 0.026,   # V1
    "ghost_product": 0.020,   # V2  (item-level)
    "price_tamper" : 0.025,   # V3  (item-level)
    "bad_total"    : 0.025,   # V5
}

# email_snapshot drift rate — simulates customers whose email changed after placing
# the order. This is not a defect (the order was valid when placed) but it demonstrates
# why email_snapshot is untrustworthy and why the pipeline removes it in favour of
# the canonical email in the customers table.
EMAIL_DRIFT_RATE = 0.15

# ── Output paths ─────────────────────────────────────────────────────────────────
# Mirrors the S3 landing key structure: landing/<interval>/<schema>/<table>/
BASE    = "landing"
LANDING = f"{BASE}/{interval_start_str}"

PATHS = {
    "customers"  : f"{LANDING}/customers",
    "products"   : f"{LANDING}/products",
    "orders" : f"{LANDING}/orders",
    "order_items": f"{LANDING}/order_items",
}
for p in PATHS.values():
    os.makedirs(p, exist_ok=True)

print(f"Generating data for interval: {interval_start_str}")
print(f"Output root: {BASE}/{interval_start_str}/\n")

# ══════════════════════════════════════════════════════════════════════════════
# 1. CUSTOMERS
# ══════════════════════════════════════════════════════════════════════════════
print("▶  Generating customers...")
LOYALTY_TIERS   = ["bronze", "silver", "gold", "platinum"]
LOYALTY_WEIGHTS = [0.50,      0.28,     0.15,    0.07]

customers = []
for i in range(1, N_CUSTOMERS + 1):
    customers.append({
        "customer_id"       : f"CUST-{uuid.uuid4()}",
        "first_name"        : fake.first_name(),
        "last_name"         : fake.last_name(),
        "email"             : fake.unique.email(),
        "phone"             : fake.phone_number(),
        "city"              : fake.city(),
        "state"             : fake.state_abbr(),
        "zip_code"          : fake.zipcode(),
        "loyalty_tier"      : random.choices(LOYALTY_TIERS, LOYALTY_WEIGHTS)[0],
        "loyalty_points"    : random.randint(0, 12000),
        "joined_date"       : fake.date_between(start_date="-5y", end_date="-30d").isoformat(),
        "is_active"         : random.choices([True, False], [0.93, 0.07])[0],
        # source system metadata — not promoted to transactions
        "acquisition_channel": random.choice(["organic", "paid_search", "referral", "social", "email_campaign"]),
        "referral_code"     : fake.bothify("REF-####") if random.random() < 0.2 else None,
        "marketing_opt_in"  : random.choices([True, False], [0.65, 0.35])[0],
        "account_status_code": random.choice(["A001", "A001", "A001", "S002", "D003"]),
        "last_login_at"     : fake.date_time_between(start_date="-90d", end_date="now").isoformat() + "Z",
        # schema drift columns — only present when --schema-drift flag is set
        **({
            "preferred_contact_method": random.choice(["email", "sms", "phone"]),
        } if schema_drift else {}),
    })

df_customers = pd.DataFrame(customers)
df_customers.to_csv(f"{PATHS['customers']}/customers.csv", index=False)
print(f"   ✔  {len(df_customers):,} customers")

valid_cust_ids = df_customers["customer_id"].tolist()

# ══════════════════════════════════════════════════════════════════════════════
# 2. PRODUCTS
# ══════════════════════════════════════════════════════════════════════════════
print("▶  Generating products...")
CATEGORIES = {
    "Electronics"  : (49.99,  899.99),
    "Clothing"     : (9.99,   149.99),
    "Home & Garden": (14.99,  399.99),
    "Sports"       : (19.99,  299.99),
    "Books"        : (4.99,    49.99),
    "Toys"         : (7.99,    89.99),
    "Beauty"       : (5.99,    79.99),
    "Automotive"   : (9.99,   249.99),
}

products = []
for i in range(1, N_PRODUCTS + 1):
    category = random.choice(list(CATEGORIES.keys()))
    lo, hi   = CATEGORIES[category]
    price    = round(random.uniform(lo, hi), 2)
    products.append({
        "product_id"       : f"PROD-{uuid.uuid4()}",
        "product_name"     : fake.catch_phrase().title(),
        "category"         : category,
        "unit_price"       : price,
        "weight_kg"        : round(random.uniform(0.1, 25.0), 3),
        "stock_quantity"   : random.randint(0, 500),
        "is_active"        : random.choices([True, False], [0.90, 0.10])[0],
        "created_date"     : fake.date_between(start_date="-3y", end_date="-60d").isoformat(),
        # source system metadata — not promoted to transactions
        "sku"              : fake.bothify("SKU-???-####").upper(),
        "cost_price"       : round(price * random.uniform(0.35, 0.65), 2),
        "supplier_code"    : fake.bothify("SUP-####"),
        "warehouse_bin"    : fake.bothify("BIN-??-##").upper(),
        "reorder_threshold": random.randint(5, 50),
    })

df_products  = pd.DataFrame(products)
df_products.to_csv(f"{PATHS['products']}/products.csv", index=False)
price_lookup = df_products.set_index("product_id")["unit_price"].to_dict()
valid_prod_ids = df_products["product_id"].tolist()
print(f"   ✔  {len(df_products):,} products")

# ══════════════════════════════════════════════════════════════════════════════
# 3. ORDER ITEMS  (clean first)
# ══════════════════════════════════════════════════════════════════════════════
print("▶  Generating orders and order items...")
STATUSES        = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "gift_card"]

skeletons      = []
order_items    = []
order_item_map = {}
item_seq       = 1

for i in range(1, N_ORDERS + 1):
    oid = f"ORD-{uuid.uuid4()}"
    cid = random.choice(valid_cust_ids)

    n_items = random.choices([1, 2, 3, 4, 5], [0.30, 0.35, 0.20, 0.10, 0.05])[0]
    items   = []
    for _ in range(n_items):
        prod_id    = random.choice(valid_prod_ids)
        qty        = random.randint(1, 8)
        unit_price = price_lookup[prod_id]
        discount   = round(
            random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20]) * unit_price * qty, 2
        )
        line_total = round(qty * unit_price - discount, 2)
        items.append({
            "item_id"   : f"ITEM-{uuid.uuid4()}",
            "order_id"  : oid,
            "product_id": prod_id,
            "quantity"  : qty,
            "unit_price": unit_price,
            "discount"  : discount,
            "line_total": line_total,
            # source system metadata — not promoted to transactions
            "promo_applied"        : discount > 0,
            "fulfillment_center_id": fake.bothify("FC-##"),
            "picking_status_code"  : random.choice(["PICK_PENDING", "PICK_COMPLETE", "PICK_FAILED"]),
        })
        item_seq += 1

    order_item_map[oid] = items
    order_items.extend(items)

    skeletons.append({
        "order_id"      : oid,
        "customer_id"   : cid,
        # email_snapshot: 15% of orders carry a stale/old email — the customer's
        # email changed after placing the order. This is why the pipeline strips
        # email_snapshot and joins to customers for the canonical email instead.
        "email_snapshot": (
            fake.email()  # drifted — customer's email has since changed
            if random.random() < EMAIL_DRIFT_RATE
            else df_customers.set_index("customer_id")["email"].get(cid, fake.email())
        ),
        "order_date"    : fake.date_between(start_date="-2y", end_date="today").isoformat(),
        "status"        : random.choices(STATUSES, [0.05, 0.10, 0.20, 0.60, 0.05])[0],
        "payment_method": random.choice(PAYMENT_METHODS),
        # declared_total starts as the correct computed total; defects may corrupt it
        "declared_total": round(sum(it["line_total"] for it in items), 2),
        "currency"      : "USD",
        "source_system" : random.choice(["web", "mobile_ios", "mobile_android", "pos"]),
        "ingested_at"   : datetime.utcnow().isoformat() + "Z",
        # source system metadata — not promoted to transactions
        "billing_address"   : fake.address().replace("\n", ", "),
        "shipping_address"  : fake.address().replace("\n", ", "),
        "promo_code"        : fake.bothify("PROMO-####") if random.random() < 0.18 else None,
        "session_id"        : fake.uuid4(),
        "ip_address"        : fake.ipv4(),
        "raw_status_code"   : random.choice(["ORD_001", "ORD_002", "ORD_003", "ORD_004", "ORD_005"]),
        "user_agent"        : fake.user_agent(),
        # schema drift columns — only present when --schema-drift flag is set
        **({
            "delivery_window": random.choice(["1-2 days", "2-3 days", "3-5 days", "5-7 days"]),
            "gift_message"   : random.choices([True, False], [0.12, 0.88])[0],
        } if schema_drift else {}),
    })

print(f"   ✔  {len(skeletons):,} orders  |  {len(order_items):,} order items")

# ══════════════════════════════════════════════════════════════════════════════
# 4. INJECT DEFECTS
# ══════════════════════════════════════════════════════════════════════════════
print("▶  Injecting defects...")
GHOST_CUST_IDS = [f"CUST-GHOST-{i:03d}" for i in range(1, 11)]
GHOST_PROD_IDS = [f"PROD-GHOST-{i:03d}" for i in range(1, 6)]

defect_counts = {
    "V1_UNKNOWN_CUSTOMER": 0,
    "V2_UNKNOWN_PRODUCT" : 0,
    "V3_PRICE_MISMATCH"  : 0,
    "V5_BAD_TOTAL"       : 0,
    "EMAIL_DRIFT"        : 0,   # not a validation failure — just stale denormalized data
}

# Count email drift (set during skeleton generation)
cust_email_lookup = df_customers.set_index("customer_id")["email"]
for order in skeletons:
    real_email = cust_email_lookup.get(order["customer_id"], "")
    if order["email_snapshot"] != real_email:
        defect_counts["EMAIL_DRIFT"] += 1

for order in skeletons:
    if random.random() < DEFECT_RATES["bad_customer"]:
        order["customer_id"] = random.choice(GHOST_CUST_IDS)
        defect_counts["V1_UNKNOWN_CUSTOMER"] += 1

    if random.random() < DEFECT_RATES["bad_total"]:
        order["declared_total"] = round(
            order["declared_total"] * random.choice([-1, 0.3, 3.5]), 2
        )
        defect_counts["V5_BAD_TOTAL"] += 1

for item in order_items:
    if random.random() < DEFECT_RATES["ghost_product"]:
        item["product_id"] = random.choice(GHOST_PROD_IDS)
        defect_counts["V2_UNKNOWN_PRODUCT"] += 1
    elif random.random() < DEFECT_RATES["price_tamper"]:
        item["unit_price"] = round(item["unit_price"] * random.choice([0.4, 1.8, 2.5]), 2)
        defect_counts["V3_PRICE_MISMATCH"] += 1

for code, count in defect_counts.items():
    print(f"   ⚠  {code:<30} {count:>4} records")

# ══════════════════════════════════════════════════════════════════════════════
# 5. WRITE LANDING FILES
# ══════════════════════════════════════════════════════════════════════════════
print("▶  Writing landing files...")

# orders → newline-delimited JSON (Glue reads with spark.read.json())
orders_path = f"{PATHS['orders']}/orders.json"
with open(orders_path, "w") as f:
    for record in skeletons:
        f.write(json.dumps(record) + "\n")

# order_items → CSV
order_items_path = f"{PATHS['order_items']}/order_items.csv"
pd.DataFrame(order_items).to_csv(order_items_path, index=False)

print(f"   ✔  {PATHS['customers']}/customers.csv")
print(f"   ✔  {PATHS['products']}/products.csv")
print(f"   ✔  {orders_path}")
print(f"   ✔  {order_items_path}")

# ══════════════════════════════════════════════════════════════════════════════
# 6. MANIFEST
# ══════════════════════════════════════════════════════════════════════════════
manifest = {
    "generated_at"      : datetime.utcnow().isoformat() + "Z",
    "data_interval_start": interval_start_str,
    "landing_root"      : f"{BASE}/{interval_start_str}/",
    "tables": {
        "customers"  : {"rows": N_CUSTOMERS,      "format": "csv",    "file": "customers.csv"},
        "products"   : {"rows": N_PRODUCTS,       "format": "csv",    "file": "products.csv"},
        "orders" : {"rows": N_ORDERS,         "format": "ndjson", "file": "orders.json"},
        "order_items": {"rows": len(order_items), "format": "csv",    "file": "order_items.csv"},
    },
    "schema_drift"     : schema_drift,
    "new_columns_if_drift": {
        "orders"   : ["delivery_window", "gift_message"],
        "customers": ["preferred_contact_method"],
    } if schema_drift else {},
    "defects_injected": defect_counts,
    "validation_rules": {
        "V1": "customer_id must exist in customers",
        "V2": "product_id must exist in products",
        "V3": "unit_price in order_items must match catalogue price within 1%",
        "V5": "declared_total must match sum of line items within 30%",
    },
}

manifest_path = f"{BASE}/{interval_start_str}/manifest.json"
with open(manifest_path, "w") as f:
    json.dump(manifest, f, indent=2)

print(f"\n{'=' * 60}")
print(f"  GENERATION COMPLETE — interval: {interval_start_str}")
print(f"{'=' * 60}")
print(f"  customers    {N_CUSTOMERS:>6,} rows   [csv]")
print(f"  products     {N_PRODUCTS:>6,} rows   [csv]")
print(f"  orders       {N_ORDERS:>6,} rows   [ndjson]")
print(f"  order_items  {len(order_items):>6,} rows   [csv]")
print(f"\n  Defects:")
for code, count in defect_counts.items():
    print(f"    {code:<30} {count:>4}")
print(f"\n  Manifest: {manifest_path}")
print(f"{'=' * 60}")