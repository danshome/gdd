#!/usr/bin/env python3
"""Generate a small (~1MB) fixture SQLite database for frontend tests.

Creates tests/fixture.sqlite with representative data:
- ~2000 readings covering 7 days of 5-minute data
- 3 days of real station data, 2 days interpolated, 2 days forecast
- All 8 grapevine varieties with mock predictions
- 15 pest entries
- A few sunspot records
- Pre-calculated GDD values for self-consistency
"""

import sqlite3
import os
import math
from datetime import datetime, timedelta, timezone

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), "fixture.sqlite")
BASE_TEMP_C = 10
INTERVALS_PER_DAY = 288  # 24*60/5


def create_schema(cursor):
    """Create all tables matching the real database schema."""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS readings (
        dateutc INTEGER PRIMARY KEY,
        date TEXT,
        tempf REAL,
        humidity REAL,
        baromrelin REAL,
        baromabsin REAL,
        feelsLike REAL,
        dewPoint REAL,
        winddir REAL,
        windspeedmph REAL,
        windgustmph REAL,
        maxdailygust REAL,
        windgustdir REAL,
        winddir_avg2m REAL,
        windspdmph_avg2m REAL,
        winddir_avg10m REAL,
        windspdmph_avg10m REAL,
        hourlyrainin REAL,
        dailyrainin REAL,
        monthlyrainin REAL,
        yearlyrainin REAL,
        battin REAL,
        battout REAL,
        tempinf REAL,
        humidityin REAL,
        feelsLikein REAL,
        dewPointin REAL,
        lastRain TEXT,
        passkey TEXT,
        time INTEGER,
        loc TEXT,
        gdd REAL DEFAULT 0,
        gdd_hourly REAL DEFAULT 0,
        gdd_daily REAL DEFAULT 0,
        is_generated INTEGER DEFAULT 0,
        mac_source TEXT DEFAULT NULL
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_readings_day ON readings (substr(date, 1, 10))")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gdd ON readings (gdd)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_readings_year ON readings((substr(date, 1, 4)))")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_readings_date ON readings (date)")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS grapevine_gdd (
        variety TEXT PRIMARY KEY,
        heat_summation INTEGER,
        biofix_date TEXT DEFAULT (date('now','start of year')),
        gdd REAL DEFAULT 0,
        regression_projected_bud_break TEXT,
        hybrid_projected_bud_break TEXT,
        hybrid_bud_break_range TEXT,
        ehml_projected_bud_break TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vineyard_pests (
        sequence_id INTEGER PRIMARY KEY,
        common_name TEXT,
        scientific_name TEXT,
        dormant INTEGER CHECK (dormant IN (0,1)),
        stage TEXT,
        min_gdd INTEGER,
        max_gdd INTEGER
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sunspots (
        year INTEGER,
        month INTEGER,
        day INTEGER,
        fraction REAL,
        daily_total INTEGER,
        std_dev REAL,
        num_obs INTEGER,
        definitive INTEGER,
        date TEXT,
        PRIMARY KEY (year, month, day)
    )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sunspots_year ON sunspots((substr(date, 1, 4)))")


def generate_temp(hour):
    """Generate a realistic temperature (°F) based on hour of day.
    Sinusoidal pattern: low ~35°F at 5am, high ~60°F at 3pm.
    """
    return 47.5 + 12.5 * math.sin(math.pi * (hour - 5) / 12)


def gdd_increment(tempf):
    """Calculate a single 5-minute GDD increment."""
    temp_c = (tempf - 32) * 5 / 9
    return max(0.0, (temp_c - BASE_TEMP_C)) / INTERVALS_PER_DAY


def insert_readings(cursor):
    """Insert ~2016 readings covering 7 days (288 per day)."""
    # Use dates in March 2025 as representative spring data
    base_date = datetime(2025, 3, 15, 0, 0, 0, tzinfo=timezone.utc)
    cumulative_gdd = 0.0

    for day_offset in range(7):
        day_dt = base_date + timedelta(days=day_offset)

        # Determine mac_source and is_generated based on day
        if day_offset < 3:
            mac_source = "B8:D8:12:60:57:97"
            is_generated = 0
        elif day_offset < 5:
            mac_source = "INTERP"
            is_generated = 1
        else:
            mac_source = "OPENMETEO"
            is_generated = 1

        for interval in range(INTERVALS_PER_DAY):
            dt = day_dt + timedelta(minutes=5 * interval)
            dateutc = int(dt.timestamp())
            date_str = dt.strftime("%Y-%m-%dT%H:%M:%S+00:00Z")
            hour = dt.hour + dt.minute / 60.0

            # Add day-to-day variation
            day_offset_temp = day_offset * 1.5
            tempf = round(generate_temp(hour) + day_offset_temp, 1)
            humidity = round(60 + 15 * math.cos(math.pi * hour / 12), 1)

            inc = gdd_increment(tempf)
            cumulative_gdd += inc

            cursor.execute("""
                INSERT INTO readings
                (dateutc, date, tempf, humidity, gdd, gdd_hourly, gdd_daily,
                 is_generated, mac_source, dailyrainin)
                VALUES (?, ?, ?, ?, ?, 0, 0, ?, ?, ?)
            """, (dateutc, date_str, tempf, humidity, round(cumulative_gdd, 6),
                  is_generated, mac_source, round(0.01 * (interval % 10 == 0), 2)))


def insert_grapevine_data(cursor):
    """Insert all 8 grape varieties with mock predictions."""
    varieties = [
        ("Chardonnay", 350),
        ("Tempranillo", 355),
        ("Sangiovese", 371),
        ("Syrah", 378),
        ("Grenache", 379),
        ("Merlot", 383),
        ("Cabernet Sauvignon", 386),
        ("Mourvedre", 409),
    ]
    current_year = datetime.now().year
    for variety, heat_sum in varieties:
        # Generate mock predictions staggered by a few days
        base_day = 85 + varieties.index((variety, heat_sum)) * 2
        reg_date = (datetime(current_year, 1, 1) + timedelta(days=base_day - 1)).strftime("%Y-%m-%d")
        hybrid_date = (datetime(current_year, 1, 1) + timedelta(days=base_day + 1)).strftime("%Y-%m-%d")
        ehml_date = (datetime(current_year, 1, 1) + timedelta(days=base_day)).strftime("%Y-%m-%d")
        range_start = (datetime(current_year, 1, 1) + timedelta(days=base_day - 5)).strftime("%Y-%m-%dT00:00:00")
        range_end = (datetime(current_year, 1, 1) + timedelta(days=base_day + 5)).strftime("%Y-%m-%dT00:00:00")
        hybrid_range = f"{range_start},{range_end}"

        cursor.execute("""
            INSERT INTO grapevine_gdd
            (variety, heat_summation, biofix_date, gdd,
             regression_projected_bud_break, hybrid_projected_bud_break,
             hybrid_bud_break_range, ehml_projected_bud_break)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (variety, heat_sum, f"{current_year}-01-01", 150.0,
              reg_date, hybrid_date, hybrid_range, ehml_date))


def insert_pest_data(cursor):
    """Insert 15 representative pest entries."""
    pests = [
        (1, "American plum borer", "Euzophera semifuneralis", 0, "A", 245, 440),
        (2, "Aphids", "Leaf and twig forms", 1, "E", 7, 120),
        (3, "Aphids", "Leaf and twig forms", 0, "N,A", 100, 200),
        (15, "Bagworm", "Thyridopteryx ephemeraeformis", 0, "L", 600, 900),
        (20, "Black Vine Weevil", "Otiorhynchus sulcatus", 0, "A", 148, 400),
        (37, "Eastern tent caterpillar", "Malacosma americanum", 0, "L", 90, 190),
        (55, "Gypsy moth", "Lymantria dispar", 0, "L", 90, 448),
        (66, "Japanese beetle", "Popillia japonica", 0, "A", 1029, 2154),
        (67, "Juniper scale", "Carulaspis juniperi", 0, "C", 707, 1260),
        (80, "Mountain ash sawfly", "Pristiphora geniculata", 0, "L", 448, 707),
        (90, "Oystershell scale", "Lepidosaphes ulmi", 0, "C", 363, 707),
        (100, "Pine sawflies", "Diprion spp., Neodiprion spp.", 0, "L", 246, 1388),
        (113, "Rose chafer", "Macrodactylus subspinosus", 0, "A", 448, 802),
        (137, "Twospotted spider mite", "Tetranychus urticae", 0, "N,A", 363, 618),
        (148, "Zimmerman pine moth", "Dioryctria zimmermani", 0, "L", 121, 246),
    ]
    for p in pests:
        cursor.execute("""
            INSERT INTO vineyard_pests
            (sequence_id, common_name, scientific_name, dormant, stage, min_gdd, max_gdd)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, p)


def insert_sunspot_data(cursor):
    """Insert a few sunspot records."""
    sunspots = [
        (2025, 1, 1, 2025.003, 150, 10.5, 25, 0, "2025-01-01"),
        (2025, 1, 15, 2025.041, 140, 9.8, 23, 0, "2025-01-15"),
        (2025, 2, 1, 2025.085, 145, 11.2, 24, 0, "2025-02-01"),
        (2025, 2, 15, 2025.126, 155, 12.1, 26, 0, "2025-02-15"),
        (2025, 3, 1, 2025.164, 160, 10.0, 22, 0, "2025-03-01"),
    ]
    for s in sunspots:
        cursor.execute("""
            INSERT INTO sunspots
            (year, month, day, fraction, daily_total, std_dev, num_obs, definitive, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, s)


def main():
    if os.path.exists(FIXTURE_PATH):
        os.remove(FIXTURE_PATH)

    conn = sqlite3.connect(FIXTURE_PATH)
    cursor = conn.cursor()

    create_schema(cursor)
    insert_readings(cursor)
    insert_grapevine_data(cursor)
    insert_pest_data(cursor)
    insert_sunspot_data(cursor)

    conn.commit()

    # Verify
    cursor.execute("SELECT COUNT(*) FROM readings")
    readings_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM grapevine_gdd")
    varieties_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM vineyard_pests")
    pests_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM sunspots")
    sunspots_count = cursor.fetchone()[0]

    conn.close()

    size_kb = os.path.getsize(FIXTURE_PATH) / 1024
    print(f"Created {FIXTURE_PATH}")
    print(f"  Readings: {readings_count}")
    print(f"  Varieties: {varieties_count}")
    print(f"  Pests: {pests_count}")
    print(f"  Sunspots: {sunspots_count}")
    print(f"  Size: {size_kb:.0f} KB")


if __name__ == "__main__":
    main()
