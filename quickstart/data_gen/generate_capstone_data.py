#!/usr/bin/env python3
"""
RetailPulse Capstone Data Generator
=====================================
Generates realistic retail datasets for the RetailPulse PySpark Capstone project.

Two modes
---------
  full  – production-scale data (2 years, ~500 K orders)  [default]
  dev   – dev/quick-start sample   (3 months,  ~10 K orders)

Output layout (inside <output_dir>)
-------------------------------------
  batch/
    orders.csv          – historical transactional data   (large, partitioned hint)
    products.json       – product catalogue               (small, broadcast candidate)
    customers.csv       – customer master                 (medium)
    regions.csv         – region metadata                 (tiny lookup)
  streaming/
    input/
      batch_001.json .. batch_NNN.json   – micro-batch event files
                                           (drop into watch-dir to trigger streaming)

Data characteristics (intentionally built-in for capstone exercises)
----------------------------------------------------------------------
  * DATA SKEW     – "East" region carries ~35 % of order volume
  * SEASONALITY   – Nov / Dec get 3× normal daily volume (Q1 is 0.7×)
  * ANOMALIES     – ~1 % of orders are high-value (>$5,000) for alert detection
  * LATE EVENTS   – ~5 % of streaming batch events arrive with a 10-20 min delay
  * NULL / DIRTY  – ~0.5 % of non-key order fields are intentionally null
  * REPRODUCIBLE  – fixed random seed (42) → same data every run

Usage
-----
  python generate_capstone_data.py                       # full mode, default output
  python generate_capstone_data.py --mode dev            # dev sample
  python generate_capstone_data.py --mode full --output /tmp/retailpulse
  python generate_capstone_data.py --mode dev  --output ./my_data
  python generate_capstone_data.py --help

Compatibility
-------------
  Python 3.8+  |  Standard-library only (no pip install required)

Run inside Docker
-----------------
  docker exec spark-master python3 /opt/spark/jobs/data_gen/generate_capstone_data.py --mode dev
"""

import argparse
import csv
import json
import math
import os
import random
import shutil
import sys
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

SEED = 42  # fixed seed → reproducible for all team members

REGIONS = ["North", "South", "East", "West", "Central"]
# East is intentionally skewed (data-skew exercise on Day-11)
REGION_WEIGHTS = [0.18, 0.17, 0.35, 0.17, 0.13]

SEGMENTS = ["Retail", "Wholesale", "Corporate"]
SEGMENT_WEIGHTS = [0.60, 0.25, 0.15]

LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
LOYALTY_WEIGHTS = [0.45, 0.30, 0.18, 0.07]

PAYMENT_METHODS = ["Credit Card", "Debit Card", "UPI", "Cash", "Net Banking"]
PAYMENT_WEIGHTS = [0.30, 0.25, 0.25, 0.10, 0.10]

ORDER_STATUSES = ["Completed", "Returned", "Cancelled", "Pending"]
STATUS_WEIGHTS = [0.80, 0.10, 0.07, 0.03]

# Product catalogue definition
# Format: (category, subcategory, [(product_name, base_price), ...])
PRODUCT_CATALOGUE = [
    ("Electronics", "Smartphones",  [("Samsung Galaxy S24", 999),   ("iPhone 16",    1199),
                                      ("Motorola Edge 40", 499),     ("OnePlus 12",    599)]),
    ("Electronics", "Laptops",      [("Dell XPS 15",      1499),    ("MacBook Pro 14",1999),
                                      ("Lenovo ThinkPad", 899),      ("HP Pavilion",   699)]),
    ("Electronics", "Tablets",      [("iPad Air",          749),     ("Galaxy Tab S9",  649),
                                      ("Lenovo Tab P12",   499),     ("Amazon Fire HD", 229)]),
    ("Electronics", "Audio",        [("Sony WH-1000XM5",  349),     ("Bose QC45",      329),
                                      ("JBL Charge 5",     179),     ("AirPods Pro",    249)]),
    ("Electronics", "Accessories",  [("USB-C Hub",          59),     ("Wireless Charger", 39),
                                      ("Screen Protector",  19),     ("Phone Case",       15)]),
    ("Clothing",    "Men's Wear",   [("Formal Shirt",        45),    ("Chino Trousers",   55),
                                      ("Polo T-Shirt",       30),    ("Denim Jeans",      60)]),
    ("Clothing",    "Women's Wear", [("Kurti",               35),    ("Saree",            89),
                                      ("Formal Blouse",      40),    ("Leggings",         25)]),
    ("Clothing",    "Footwear",     [("Running Shoes",       85),    ("Formal Shoes",     95),
                                      ("Casual Sneakers",    70),    ("Sandals",          35)]),
    ("Clothing",    "Winter Wear",  [("Woollen Jacket",     120),    ("Fleece Hoodie",    65),
                                      ("Thermal Innerwear",  30),    ("Knit Cap",         15)]),
    ("Groceries",   "Staples",      [("Basmati Rice 5kg",    25),    ("Wheat Flour 10kg", 18),
                                      ("Refined Oil 5L",     35),    ("Toor Dal 2kg",     12)]),
    ("Groceries",   "Dairy",        [("Full Cream Milk 2L",   5),    ("Cheddar Cheese",   12),
                                      ("Butter 500g",          8),    ("Yoghurt 1kg",       6)]),
    ("Groceries",   "Snacks",       [("Potato Chips 200g",    3),    ("Mixed Nuts 500g",  15),
                                      ("Biscuits Pack",        4),    ("Instant Noodles",   2)]),
    ("Groceries",   "Beverages",    [("Green Tea 100 bags",  12),    ("Coffee 250g",      14),
                                      ("Energy Drink 6pk",   18),    ("Mineral Water 12pk",10)]),
    ("Furniture",   "Seating",      [("Office Chair",        299),   ("Sofa 3-Seater",    799),
                                      ("Bar Stool",           89),    ("Bean Bag",          49)]),
    ("Furniture",   "Storage",      [("Bookshelf 5-tier",   159),    ("Wardrobe 3-door",  449),
                                      ("Shoe Rack",           69),    ("Storage Ottoman",   85)]),
    ("Furniture",   "Tables",       [("Dining Table 6-seat",549),    ("Coffee Table",     149),
                                      ("Study Desk",          199),   ("Side Table",        79)]),
    ("Sports",      "Fitness",      [("Yoga Mat",             25),    ("Resistance Bands",  15),
                                      ("Dumbbell Set 20kg",   89),    ("Pull-Up Bar",       35)]),
    ("Sports",      "Outdoor",      [("Camping Tent",        199),    ("Trekking Backpack", 79),
                                      ("Sleeping Bag",         55),    ("Hydration Flask",  28)]),
    ("Sports",      "Team Sports",  [("Cricket Bat",          75),    ("Football",          30),
                                      ("Badminton Set",        40),    ("Table Tennis Set",  55)]),
    ("Beauty",      "Skincare",     [("Moisturiser SPF50",   28),    ("Vitamin C Serum",   35),
                                      ("Sunscreen SPF30",     18),    ("Face Wash",         12)]),
    ("Beauty",      "Haircare",     [("Shampoo 400ml",       10),    ("Conditioner 400ml",  9),
                                      ("Hair Oil 200ml",       8),    ("Hair Mask",         14)]),
    ("Beauty",      "Makeup",       [("Lipstick",             18),    ("Foundation",         25),
                                      ("Mascara",              20),    ("Compact Powder",    15)]),
    ("Books",       "Technology",   [("Clean Code",           35),    ("Designing Data-Intensive Apps", 55),
                                      ("The Pragmatic Programmer", 42), ("Python Cookbook", 48)]),
    ("Books",       "Business",     [("Atomic Habits",        18),    ("Zero to One",       20),
                                      ("Good to Great",         22),   ("Deep Work",         17)]),
    ("Books",       "Fiction",      [("The Alchemist",        12),    ("1984",               10),
                                      ("Sapiens",              15),    ("Dune",               14)]),
]

FIRST_NAMES = [
    "Aarav", "Priya", "Rohan", "Sneha", "Vikram", "Anjali", "Arjun", "Meera",
    "Kiran", "Divya", "Rahul", "Pooja", "Amit", "Nisha", "Raj", "Sunita",
    "Sanjay", "Kavya", "Nikhil", "Shreya", "Vishal", "Riya", "Deepak", "Ananya",
    "Suresh", "Lakshmi", "Ramesh", "Geetha", "Mahesh", "Usha",
    "James", "Emma", "Oliver", "Sophia", "Liam", "Ava", "Noah", "Isabella",
    "William", "Mia", "Benjamin", "Charlotte", "Lucas", "Amelia", "Mason", "Harper",
    "Chen", "Wei", "Aisha", "Omar", "Fatima", "Ali", "Sara", "Yusuf",
]

LAST_NAMES = [
    "Sharma", "Patel", "Singh", "Kumar", "Verma", "Gupta", "Rao", "Reddy",
    "Nair", "Pillai", "Iyer", "Menon", "Joshi", "Mehta", "Shah", "Malhotra",
    "Bose", "Banerjee", "Chatterjee", "Mukherjee", "Das", "Sen", "Ghosh",
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Miller", "Wilson",
    "Anderson", "Taylor", "Thomas", "Jackson", "White", "Harris", "Martin", "Garcia",
    "Khan", "Ali", "Ahmed", "Hassan", "Ibrahim", "Abdullah",
    "Wang", "Li", "Zhang", "Liu", "Chen", "Yang",
]

CITIES_BY_REGION = {
    "North":   ["Delhi", "Chandigarh", "Jaipur", "Lucknow", "Amritsar", "Shimla"],
    "South":   ["Bengaluru", "Chennai", "Hyderabad", "Kochi", "Coimbatore", "Mysuru"],
    "East":    ["Kolkata", "Patna", "Bhubaneswar", "Guwahati", "Ranchi", "Dhanbad"],
    "West":    ["Mumbai", "Pune", "Ahmedabad", "Surat", "Nagpur", "Vadodara"],
    "Central": ["Bhopal", "Indore", "Raipur", "Nagpur", "Jabalpur", "Gwalior"],
}

SUPPLIERS = [f"SUP-{str(i).zfill(3)}" for i in range(1, 51)]


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def weighted_choice(rng: random.Random, population: list, weights: list):
    """Single weighted random choice."""
    cumulative = []
    total = 0
    for w in weights:
        total += w
        cumulative.append(total)
    r = rng.random() * total
    for item, cum in zip(population, cumulative):
        if r <= cum:
            return item
    return population[-1]


def seasonal_multiplier(date: datetime) -> float:
    """Return a volume multiplier based on the month (simulates seasonal demand)."""
    month = date.month
    if month in (11, 12):   # holiday season
        return 3.0
    elif month in (1, 2):   # post-holiday slump
        return 0.7
    elif month in (6, 7):   # mid-year sale
        return 1.5
    return 1.0


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def progress(msg: str):
    print(f"  [+] {msg}", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# GENERATORS
# ─────────────────────────────────────────────────────────────────────────────

def generate_products(n: int, rng: random.Random) -> list[dict]:
    """
    Generate product catalogue.
    All canonical products from PRODUCT_CATALOGUE are always included;
    remaining slots are filled with synthetic variants.
    """
    products = []
    pid = 1

    # 1. Canonical products (guaranteed to appear)
    for cat, subcat, items in PRODUCT_CATALOGUE:
        for name, base_price in items:
            products.append({
                "product_id":     f"PROD-{str(pid).zfill(5)}",
                "product_name":   name,
                "category":       cat,
                "subcategory":    subcat,
                "base_price":     round(base_price * rng.uniform(0.95, 1.05), 2),
                "supplier_id":    rng.choice(SUPPLIERS),
                "stock_quantity": rng.randint(10, 5000),
                "is_active":      True,
                "reorder_level":  rng.randint(5, 100),
            })
            pid += 1

    # 2. Synthetic filler to reach n
    while len(products) < n:
        cat, subcat, items = rng.choice(PRODUCT_CATALOGUE)
        base_name, base_price = rng.choice(items)
        products.append({
            "product_id":     f"PROD-{str(pid).zfill(5)}",
            "product_name":   f"{base_name} (Variant-{pid})",
            "category":       cat,
            "subcategory":    subcat,
            "base_price":     round(base_price * rng.uniform(0.8, 1.5), 2),
            "supplier_id":    rng.choice(SUPPLIERS),
            "stock_quantity": rng.randint(0, 5000),
            "is_active":      rng.random() > 0.05,   # 5% inactive
            "reorder_level":  rng.randint(5, 100),
        })
        pid += 1

    rng.shuffle(products)
    progress(f"Generated {len(products)} products")
    return products


def generate_customers(n: int, rng: random.Random) -> list[dict]:
    """Generate customer master data."""
    customers = []
    base_date = datetime(2018, 1, 1)

    for i in range(1, n + 1):
        region = weighted_choice(rng, REGIONS, REGION_WEIGHTS)
        fname = rng.choice(FIRST_NAMES)
        lname = rng.choice(LAST_NAMES)
        name = f"{fname} {lname}"
        email_user = f"{fname.lower()}.{lname.lower()}{i}"
        domain = rng.choice(["gmail.com", "yahoo.com", "outlook.com",
                              "company.in", "retailcorp.com"])
        join_days = rng.randint(0, (datetime(2024, 6, 30) - base_date).days)
        join_date = base_date + timedelta(days=join_days)

        customers.append({
            "customer_id":   f"CUST-{str(i).zfill(6)}",
            "customer_name": name,
            "email":         f"{email_user}@{domain}",
            "phone":         f"+91-{rng.randint(7000000000, 9999999999)}",
            "region":        region,
            "city":          rng.choice(CITIES_BY_REGION[region]),
            "segment":       weighted_choice(rng, SEGMENTS, SEGMENT_WEIGHTS),
            "loyalty_tier":  weighted_choice(rng, LOYALTY_TIERS, LOYALTY_WEIGHTS),
            "join_date":     fmt_date(join_date),
            "is_active":     "true" if rng.random() > 0.03 else "false",
        })

    progress(f"Generated {len(customers)} customers")
    return customers


def generate_orders(
    target_count: int,
    rng: random.Random,
    customers: list[dict],
    products: list[dict],
    start_date: datetime,
    end_date: datetime,
) -> list[dict]:
    """
    Generate historical batch orders.

    Volume is modulated by seasonal_multiplier so daily counts naturally vary.
    ~1 % of orders are deliberately high-value (>$5,000) for anomaly exercises.
    ~0.5 % of non-key fields are set to None (dirty data simulation).
    """
    orders = []
    total_days = (end_date - start_date).days + 1
    base_daily = target_count / total_days

    oid = 1
    current = start_date

    # Index products by category for faster lookup in joins
    cat_products: dict[str, list] = {}
    for p in products:
        cat_products.setdefault(p["category"], []).append(p)

    while current <= end_date and len(orders) < target_count * 1.02:
        daily = max(1, int(base_daily * seasonal_multiplier(current) * rng.uniform(0.7, 1.3)))
        daily = min(daily, target_count - len(orders) + 1)

        for _ in range(daily):
            cust = rng.choice(customers)
            region = cust["region"]   # orders inherit customer's region (with slight spillover)
            if rng.random() < 0.08:   # 8 % cross-region orders (realistic)
                region = weighted_choice(rng, REGIONS, REGION_WEIGHTS)

            prod = rng.choice(products)
            qty = rng.randint(1, 10)

            # ~1 % high-value anomalies
            if rng.random() < 0.01:
                unit_price = round(rng.uniform(5001, 15000), 2)
            else:
                unit_price = round(prod["base_price"] * rng.uniform(0.9, 1.1), 2)

            discount = round(rng.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20]), 2)
            total_amount = round(qty * unit_price * (1 - discount), 2)

            order_hour = rng.randint(6, 22)
            order_min  = rng.randint(0, 59)
            order_ts   = current + timedelta(hours=order_hour, minutes=order_min)

            row = {
                "order_id":       f"ORD-{str(oid).zfill(8)}",
                "customer_id":    cust["customer_id"],
                "product_id":     prod["product_id"],
                "product_name":   prod["product_name"],
                "category":       prod["category"],
                "subcategory":    prod["subcategory"],
                "region":         region,
                "city":           cust["city"],
                "segment":        cust["segment"],
                "order_date":     fmt_ts(order_ts),
                "order_year":     order_ts.year,
                "order_month":    order_ts.month,
                "order_quarter":  math.ceil(order_ts.month / 3),
                "quantity":       qty,
                "unit_price":     unit_price,
                "discount":       discount,
                "total_amount":   total_amount,
                "status":         weighted_choice(rng, ORDER_STATUSES, STATUS_WEIGHTS),
                "payment_method": weighted_choice(rng, PAYMENT_METHODS, PAYMENT_WEIGHTS),
                "is_high_value":  "true" if total_amount > 5000 else "false",
            }

            # Introduce ~0.5 % dirty nulls on non-key fields
            nullable_fields = ["discount", "city", "payment_method"]
            for field in nullable_fields:
                if rng.random() < 0.005:
                    row[field] = ""

            orders.append(row)
            oid += 1

        current += timedelta(days=1)

    # Trim to exactly target_count
    orders = orders[:target_count]
    progress(f"Generated {len(orders)} orders  ({start_date.date()} → {end_date.date()})")
    return orders


def generate_regions() -> list[dict]:
    """Static region dimension table (tiny lookup, useful for broadcast join demo)."""
    return [
        {"region": "North",   "zone": "Northern India",  "manager": "Rajesh Khanna",   "hq_city": "Delhi",     "target_revenue": 5000000},
        {"region": "South",   "zone": "Southern India",  "manager": "Priya Venkatesh", "hq_city": "Bengaluru", "target_revenue": 4500000},
        {"region": "East",    "zone": "Eastern India",   "manager": "Subir Bose",      "hq_city": "Kolkata",   "target_revenue": 6000000},
        {"region": "West",    "zone": "Western India",   "manager": "Neha Agarwal",    "hq_city": "Mumbai",    "target_revenue": 5500000},
        {"region": "Central", "zone": "Central India",   "manager": "Alok Tiwari",     "hq_city": "Bhopal",    "target_revenue": 3000000},
    ]


def generate_streaming_batches(
    n_batches: int,
    events_per_batch: int,
    rng: random.Random,
    products: list[dict],
    customers: list[dict],
    reference_time: datetime,
) -> list[list[dict]]:
    """
    Generate streaming event files.

    Each file simulates one micro-batch arriving at `reference_time + batch_idx * 1min`.
    Events inside each file have event_time close to their batch arrival time.

    Intentional characteristics for streaming exercises:
      - 5 % late events  (event_time = arrival_time – 10..20 min)  → watermark test
      - 2 % high-value   (>$5,000)                                  → alert trigger
      - Windowed revenue aggregation target   (1-min and 5-min windows)
    """
    batches = []

    for batch_idx in range(n_batches):
        arrival_time = reference_time + timedelta(minutes=batch_idx)
        events = []

        for _ in range(events_per_batch):
            cust = rng.choice(customers)
            prod = rng.choice(products)
            qty  = rng.randint(1, 5)

            is_anomaly = rng.random() < 0.02
            if is_anomaly:
                unit_price = round(rng.uniform(5001, 15000), 2)
            else:
                unit_price = round(prod["base_price"] * rng.uniform(0.9, 1.10), 2)

            total_amount = round(qty * unit_price, 2)

            # Late event injection
            is_late = rng.random() < 0.05
            if is_late:
                lag_minutes = rng.randint(10, 20)
                event_time  = arrival_time - timedelta(minutes=lag_minutes)
            else:
                event_time  = arrival_time - timedelta(seconds=rng.randint(0, 55))

            event = {
                "event_id":       f"EVT-{batch_idx:04d}-{rng.randint(10000, 99999)}",
                "order_id":       f"STR-{rng.randint(1000000, 9999999)}",
                "customer_id":    cust["customer_id"],
                "product_id":     prod["product_id"],
                "product_name":   prod["product_name"],
                "category":       prod["category"],
                "region":         cust["region"],
                "event_time":     fmt_ts(event_time),
                "arrival_time":   fmt_ts(arrival_time),
                "quantity":       qty,
                "unit_price":     unit_price,
                "total_amount":   total_amount,
                "is_late_event":  is_late,
                "is_high_value":  total_amount > 5000,
                "payment_method": weighted_choice(rng, PAYMENT_METHODS, PAYMENT_WEIGHTS),
            }
            events.append(event)

        batches.append(events)

    total_events = sum(len(b) for b in batches)
    late_events  = sum(1 for b in batches for e in b if e["is_late_event"])
    hv_events    = sum(1 for b in batches for e in b if e["is_high_value"])
    progress(
        f"Generated {n_batches} streaming batch files  "
        f"({total_events} events, {late_events} late, {hv_events} high-value)"
    )
    return batches


# ─────────────────────────────────────────────────────────────────────────────
# WRITERS
# ─────────────────────────────────────────────────────────────────────────────

def write_json_lines(data: list[dict], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for row in data:
            f.write(json.dumps(row) + "\n")


def write_csv(data: list[dict], path: str):
    if not data:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


def write_streaming_batches(batches: list[list[dict]], directory: str):
    os.makedirs(directory, exist_ok=True)
    for i, batch in enumerate(batches, start=1):
        path = os.path.join(directory, f"batch_{str(i).zfill(3)}.json")
        with open(path, "w", encoding="utf-8") as f:
            for event in batch:
                f.write(json.dumps(event) + "\n")


def write_readme(output_dir: str, mode: str, counts: dict):
    """Write a brief data-readme so teammates know what's in each file."""
    readme = f"""# RetailPulse Capstone – Generated Datasets

Mode    : {mode}
Seed    : {SEED}  (reproduce with same seed for identical data)

## Files

| File                          | Format    | Rows (~)        | Purpose                                    |
|-------------------------------|-----------|------------------|--------------------------------------------|
| batch/orders.csv              | CSV       | {counts['orders']:,}         | Historical transactional data (batch jobs) |
| batch/products.json           | JSON-lines| {counts['products']:,}           | Product catalogue (broadcast-join target)  |
| batch/customers.csv           | CSV       | {counts['customers']:,}         | Customer master                            |
| batch/regions.csv             | CSV       | 5                | Region dimension (tiny lookup table)       |
| streaming/input/batch_NNN.json| JSON-lines| {counts['streaming_events']//counts.get('streaming_batches',1)} /file        | Streaming events (drop into watch dir)     |

## Built-in Data Characteristics

| Characteristic      | Detail                                                        |
|---------------------|---------------------------------------------------------------|
| Data skew           | East region ≈ 35 % of order volume (use for Day-11 demos)     |
| Seasonality         | Nov/Dec = 3×, Jan/Feb = 0.7× normal daily volume             |
| High-value anomalies| ≈1 % orders > $5,000  (streaming alert threshold)            |
| Late streaming events | ≈5 % arrive 10-20 min delayed (watermark demo)             |
| Dirty data          | ≈0.5 % nulls on non-key fields (data quality task)           |

## Quick Spark Usage

### Batch – read orders
```python
orders_df = spark.read.option("header","true").option("inferSchema","true") \\
            .csv("/opt/spark/jobs/capstone_data/{mode}/batch/orders.csv")
```

### Batch – read products (broadcast candidate)
```python
from pyspark.sql.functions import broadcast
products_df = spark.read.json("/opt/spark/jobs/capstone_data/{mode}/batch/products.json")
joined = orders_df.join(broadcast(products_df), "product_id")
```

### Streaming – file-source streaming
```python
schema = ...   # define StructType matching streaming events
stream_df = spark.readStream.schema(schema) \\
            .json("/opt/spark/jobs/capstone_data/{mode}/streaming/input/")
```
"""
    path = os.path.join(output_dir, "README.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(readme)


# ─────────────────────────────────────────────────────────────────────────────
# PROFILE CONFIGURATIONS
# ─────────────────────────────────────────────────────────────────────────────

PROFILES = {
    "full": {
        "description":       "Production-scale – 2 years (~500 K orders)",
        "n_products":         2000,
        "n_customers":       50000,
        "n_orders":         500000,
        "start_date":        datetime(2023, 1, 1),
        "end_date":          datetime(2024, 12, 31),
        "n_streaming_batches": 60,
        "streaming_events_per_batch": 25,
    },
    "dev": {
        "description":       "Dev sample – 3 months (~10 K orders, fast generation)",
        "n_products":          200,
        "n_customers":        1000,
        "n_orders":          10000,
        "start_date":        datetime(2024, 10, 1),
        "end_date":          datetime(2024, 12, 31),
        "n_streaming_batches": 15,
        "streaming_events_per_batch": 12,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="RetailPulse Capstone Data Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--mode",
        choices=["full", "dev"],
        default="dev",
        help="Data volume profile.  'dev' is fast for local development; "
             "'full' generates production-scale data.  (default: dev)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Root output directory.  Defaults to  <script_dir>/../jobs/capstone_data/<mode>/",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete and recreate the output directory if it already exists.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=SEED,
        help=f"Random seed for reproducibility.  (default: {SEED})",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    profile = PROFILES[args.mode]

    # ── Resolve output directory ─────────────────────────────────────────────
    if args.output:
        output_dir = os.path.abspath(args.output)
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.normpath(
            os.path.join(script_dir, "..", "jobs", "capstone_data", args.mode)
        )

    # ── Header ───────────────────────────────────────────────────────────────
    print()
    print("=" * 65)
    print("  RetailPulse Capstone Data Generator")
    print("=" * 65)
    print(f"  Mode        : {args.mode}  –  {profile['description']}")
    print(f"  Output dir  : {output_dir}")
    print(f"  Seed        : {args.seed}")
    print("=" * 65)

    # ── Clean / create output directory ─────────────────────────────────────
    if args.clean and os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        progress(f"Removed existing directory: {output_dir}")

    batch_dir   = os.path.join(output_dir, "batch")
    stream_dir  = os.path.join(output_dir, "streaming", "input")
    os.makedirs(batch_dir,  exist_ok=True)
    os.makedirs(stream_dir, exist_ok=True)

    # ── Initialise RNG ───────────────────────────────────────────────────────
    rng = random.Random(args.seed)

    # ── Generate datasets ────────────────────────────────────────────────────
    print("\n  Generating master data …")
    products  = generate_products(profile["n_products"], rng)
    customers = generate_customers(profile["n_customers"], rng)
    regions   = generate_regions()

    print("\n  Generating batch orders …")
    orders = generate_orders(
        target_count = profile["n_orders"],
        rng          = rng,
        customers    = customers,
        products     = products,
        start_date   = profile["start_date"],
        end_date     = profile["end_date"],
    )

    print("\n  Generating streaming events …")
    # Reference time is "now" rounded to the last full minute for clean windows
    now = datetime.now().replace(second=0, microsecond=0)
    streaming_batches = generate_streaming_batches(
        n_batches           = profile["n_streaming_batches"],
        events_per_batch    = profile["streaming_events_per_batch"],
        rng                 = rng,
        products            = products,
        customers           = customers,
        reference_time      = now,
    )

    # ── Write to disk ────────────────────────────────────────────────────────
    print("\n  Writing files …")

    products_path  = os.path.join(batch_dir, "products.json")
    customers_path = os.path.join(batch_dir, "customers.csv")
    orders_path    = os.path.join(batch_dir, "orders.csv")
    regions_path   = os.path.join(batch_dir, "regions.csv")

    write_json_lines(products,  products_path);  progress(f"Written → {products_path}")
    write_csv(customers, customers_path);         progress(f"Written → {customers_path}")
    write_csv(orders,    orders_path);             progress(f"Written → {orders_path}")
    write_csv(regions,   regions_path);            progress(f"Written → {regions_path}")
    write_streaming_batches(streaming_batches, stream_dir)
    progress(f"Written → {stream_dir}  ({len(streaming_batches)} batch files)")

    total_streaming_events = sum(len(b) for b in streaming_batches)

    # ── README ───────────────────────────────────────────────────────────────
    write_readme(output_dir, args.mode, {
        "orders":            len(orders),
        "products":          len(products),
        "customers":         len(customers),
        "streaming_events":  total_streaming_events,
        "streaming_batches": len(streaming_batches),
    })

    # ── Summary ──────────────────────────────────────────────────────────────
    print()
    print("=" * 65)
    print("  Generation complete!")
    print("=" * 65)

    def fsize(path):
        s = os.path.getsize(path)
        return f"{s/1024:.1f} KB" if s < 1_048_576 else f"{s/1_048_576:.1f} MB"

    print(f"  {'File':<45}  {'Size':>10}")
    print(f"  {'-'*45}  {'-'*10}")
    for p in [orders_path, products_path, customers_path, regions_path]:
        print(f"  {os.path.relpath(p, output_dir):<45}  {fsize(p):>10}")

    stream_total = sum(
        os.path.getsize(os.path.join(stream_dir, f))
        for f in os.listdir(stream_dir)
    )
    total_str = (f"{stream_total/1024:.1f} KB"
                 if stream_total < 1_048_576
                 else f"{stream_total/1_048_576:.1f} MB")
    print(f"  {'streaming/input/  (' + str(len(streaming_batches)) + ' files)':<45}  {total_str:>10}")
    print()
    print(f"  Output root: {output_dir}")
    print()
    print("  Dataset breakdown:")
    print(f"    Orders              : {len(orders):>10,}")
    print(f"    Products            : {len(products):>10,}")
    print(f"    Customers           : {len(customers):>10,}")
    print(f"    Streaming events    : {total_streaming_events:>10,}  "
          f"across {len(streaming_batches)} batch files")
    print()


if __name__ == "__main__":
    main()
