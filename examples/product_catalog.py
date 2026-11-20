"""A shop publishes its catalog; a storefront reads a local copy."""

import os
from contextlib import closing
from uuid import uuid4

import redis

from redisql import RediSQL


def publish_catalog(shop: RediSQL) -> None:
    styles = ("Trail Backpack", "Daypack", "Duffel Bag", "Packing Cube", "Tote Bag")
    colors = ("blue", "red", "green")
    rows = (
        (f"{style} {model:04d}", colors[(model - 1) % len(colors)], 1999 + model)
        for style in styles
        for model in range(1, 2001)
    )

    # Leaving the batch publishes one snapshot for all 10,000 inserts.
    with shop.batch() as conn:
        conn.execute("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                color TEXT NOT NULL,
                price_cents INTEGER NOT NULL
            )
        """)
        conn.executemany("INSERT INTO products (name, color, price_cents) VALUES (?, ?, ?)", rows)


def show_cheapest_red_backpacks(storefront: RediSQL) -> None:
    rows = storefront.query(
        """
        SELECT name, color, price_cents
        FROM products
        WHERE name LIKE ? AND color = ?
        ORDER BY price_cents, id
        LIMIT 3
        """,
        ("Trail Backpack%", "red"),
    )
    for product_name, color, price_cents in rows:
        print(f"   {product_name} | {color} | ${price_cents / 100:.2f}")


def run_demo(shop: RediSQL, storefront: RediSQL) -> None:
    print("1. The shop publishes the catalog in one batch.")
    shop.start(listen=False)
    publish_catalog(shop)

    print("\n2. The storefront loads the snapshot and queries its local copy.")
    storefront.start(listen=False)
    [(count,)] = storefront.query("SELECT COUNT(*) FROM products")
    print(f"   {count:,} products loaded. Three cheapest red trail backpacks:")
    show_cheapest_red_backpacks(storefront)

    print("\n3. The shop changes Trail Backpack 0001 from blue to red.")
    shop.write(
        lambda conn: conn.execute(
            "UPDATE products SET color = ? WHERE name = ?", ("red", "Trail Backpack 0001")
        )
    )

    print("\n4. The storefront syncs and runs the same query.")
    # Sync explicitly so the example does not depend on Pub/Sub timing.
    storefront.sync()
    show_cheapest_red_backpacks(storefront)


def main() -> None:
    name = f"product-catalog-demo:{uuid4().hex}"
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    with redis.Redis.from_url(url, socket_timeout=5) as r:
        try:
            with closing(RediSQL(r, name)) as shop, closing(RediSQL(r, name)) as storefront:
                run_demo(shop, storefront)
        finally:
            r.delete(f"{name}:snap", f"{name}:ver")


if __name__ == "__main__":
    main()
