import json
import os
from typing import Set, List

import psycopg2


CATEGORY = "Construction and trades"
OUTPUT_PATH = "/home/ali/Techorphic/Projects/Pathzi/pathzi/cities.json"

TABLES = [
    "public.job_dwpjob",
    "public.course_ncscourse",
    "public.apprenticeship_apprenticeshipvacancy",
]


def fetch_cities(conn, table: str, category: str) -> List[str]:
    sql = f"""
        SELECT city
        FROM {table}
        WHERE category = %s
          AND city IS NOT NULL
          AND BTRIM(city) <> '';
    """
    with conn.cursor() as cur:
        cur.execute(sql, (category,))
        return [row[0].strip() for row in cur.fetchall() if row[0]]


def main():
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set in environment.")

    cities: Set[str] = set()

    with psycopg2.connect(db_url) as conn:
        for table in TABLES:
            for city in fetch_cities(conn, table, CATEGORY):
                cities.add(city)

    cities_sorted = sorted(cities, key=lambda s: s.lower())

    data = {
        "category": CATEGORY,
        "count": len(cities_sorted),
        "cities": cities_sorted,
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ Wrote {len(cities_sorted)} unique cities to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
