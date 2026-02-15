"""Database invariant tests against the real 623MB database.

All read-only queries. These act as a safety net — if future code changes
corrupt the data pipeline, these tests fail.
"""

import re
from datetime import datetime, timezone

import pytest


class TestReadingsInvariants:
    def test_gdd_non_negative(self, real_db):
        """GDD values should never be negative."""
        cur = real_db.cursor()
        cur.execute("SELECT COUNT(*) FROM readings WHERE gdd < 0")
        assert cur.fetchone()[0] == 0

    def test_gdd_monotonic_per_year(self, real_db):
        """Within each year, cumulative GDD should never decrease between consecutive readings."""
        cur = real_db.cursor()
        # Check for any row where GDD is less than the previous row's GDD within the same year
        cur.execute("""
            WITH ordered AS (
                SELECT substr(date, 1, 4) AS year, dateutc, gdd,
                       LAG(gdd) OVER (PARTITION BY substr(date, 1, 4) ORDER BY dateutc) AS prev_gdd
                FROM readings
                WHERE gdd > 0
            )
            SELECT COUNT(*) FROM ordered
            WHERE prev_gdd IS NOT NULL AND gdd < prev_gdd
        """)
        violations = cur.fetchone()[0]
        assert violations == 0, f"Found {violations} rows where GDD decreased within a year"

    def test_temperature_in_range(self, real_db):
        """All temperatures should be within physically plausible range [-30, 130]°F."""
        cur = real_db.cursor()
        cur.execute("SELECT COUNT(*) FROM readings WHERE tempf < -30 OR tempf > 130")
        assert cur.fetchone()[0] == 0

    def test_dates_iso_format(self, real_db):
        """All date values should match the expected ISO format."""
        cur = real_db.cursor()
        # Check a sample — full table scan of 2.7M rows is expensive
        cur.execute("SELECT date FROM readings ORDER BY RANDOM() LIMIT 1000")
        pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00Z$")
        for (date_str,) in cur.fetchall():
            assert pattern.match(date_str), f"Date does not match ISO format: {date_str}"

    def test_dateutc_matches_date(self, real_db):
        """dateutc (Unix timestamp) should convert to a date matching the date string."""
        cur = real_db.cursor()
        cur.execute("SELECT dateutc, date FROM readings ORDER BY RANDOM() LIMIT 500")
        for dateutc, date_str in cur.fetchall():
            dt_from_ts = datetime.fromtimestamp(dateutc, tz=timezone.utc)
            # Extract YYYY-MM-DD and HH:MM from the date string
            date_part = date_str[:10]
            ts_date_part = dt_from_ts.strftime("%Y-%m-%d")
            assert date_part == ts_date_part, (
                f"dateutc {dateutc} → {ts_date_part} but date column says {date_part}"
            )

    def test_readings_per_day(self, real_db):
        """Each day should have a reasonable number of readings.

        5-min intervals = 288/day. Allow >= 100 to account for gaps
        in early data. Only check days from 2024+ (recent, fully populated).
        """
        cur = real_db.cursor()
        cur.execute("""
            SELECT substr(date, 1, 10) AS day, COUNT(*) AS cnt
            FROM readings
            WHERE substr(date, 1, 4) >= '2024' AND substr(date, 1, 10) < date('now')
            GROUP BY day
            HAVING cnt < 100
        """)
        sparse_days = cur.fetchall()
        assert len(sparse_days) == 0, (
            f"Found {len(sparse_days)} days with <100 readings: {sparse_days[:5]}"
        )

    def test_mac_source_values(self, real_db):
        """mac_source should only contain known values."""
        cur = real_db.cursor()
        cur.execute("SELECT DISTINCT mac_source FROM readings WHERE mac_source IS NOT NULL")
        known = {"OPENMETEO", "INTERP", "B8:D8:12:60:57:97"}
        actual = {row[0] for row in cur.fetchall()}
        # Allow the known set plus any MAC addresses (colon-separated hex)
        mac_pattern = re.compile(r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$")
        for val in actual:
            assert val in known or mac_pattern.match(val), f"Unexpected mac_source: {val}"

    def test_generated_flag_consistency(self, real_db):
        """is_generated=1 should correlate with mac_source IN ('INTERP', 'OPENMETEO')."""
        cur = real_db.cursor()
        # Generated but not INTERP/OPENMETEO
        cur.execute("""
            SELECT COUNT(*) FROM readings
            WHERE is_generated = 1 AND mac_source NOT IN ('INTERP', 'OPENMETEO')
        """)
        assert cur.fetchone()[0] == 0, "Found generated readings with unexpected mac_source"
        # INTERP/OPENMETEO but not generated
        cur.execute("""
            SELECT COUNT(*) FROM readings
            WHERE mac_source IN ('INTERP', 'OPENMETEO') AND is_generated != 1
        """)
        assert cur.fetchone()[0] == 0, "Found INTERP/OPENMETEO readings not marked as generated"


class TestGrapevineGDDInvariants:
    def test_varieties_have_heat_summation(self, real_db):
        """All varieties should have a non-null heat_summation."""
        cur = real_db.cursor()
        cur.execute("SELECT COUNT(*) FROM grapevine_gdd WHERE heat_summation IS NULL")
        assert cur.fetchone()[0] == 0

    def test_predictions_are_valid_dates(self, real_db):
        """All projection columns should contain valid ISO dates or NULL."""
        cur = real_db.cursor()
        date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}")
        for col in ["regression_projected_bud_break", "hybrid_projected_bud_break",
                     "ehml_projected_bud_break"]:
            cur.execute(f"SELECT {col} FROM grapevine_gdd WHERE {col} IS NOT NULL")
            for (val,) in cur.fetchall():
                assert date_pattern.match(val), f"{col} has invalid date: {val}"

    def test_predictions_in_recent_year(self, real_db):
        """Prediction dates should fall within current or previous year (from last pipeline run)."""
        cur = real_db.cursor()
        current_year = datetime.now().year
        allowed_years = {current_year, current_year - 1}
        for col in ["regression_projected_bud_break", "hybrid_projected_bud_break",
                     "ehml_projected_bud_break"]:
            cur.execute(f"SELECT {col} FROM grapevine_gdd WHERE {col} IS NOT NULL")
            for (val,) in cur.fetchall():
                year = int(val[:4])
                assert year in allowed_years, (
                    f"{col} prediction year {year} not in {allowed_years}"
                )

    def test_hybrid_range_ordered(self, real_db):
        """In hybrid_bud_break_range, start_date should be <= end_date."""
        cur = real_db.cursor()
        cur.execute("SELECT variety, hybrid_bud_break_range FROM grapevine_gdd WHERE hybrid_bud_break_range IS NOT NULL")
        for variety, range_str in cur.fetchall():
            parts = range_str.split(",")
            if len(parts) == 2:
                start = parts[0].strip()
                end = parts[1].strip()
                assert start <= end, f"{variety}: range start {start} > end {end}"
