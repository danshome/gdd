"""Unit tests for GDD calculation logic.

These tests replicate the formulas from gdd.py without importing it
(to avoid config/DB dependencies). The formulas are:

GDD increment (5-min): max(0, (tempf_to_C - 10)) / 288
  where tempf_to_C = (tempf - 32) * 5 / 9
  and 288 = 24h * 60min / 5min

Chill hours: count of 5-min readings below 45°F * (5/60)

Linear regression: slope = Σ(x-x̄)(y-ȳ) / Σ(x-x̄)²
                   intercept = ȳ - slope * x̄

Interpolation: temp = temp_prev + fraction * (temp_next - temp_prev)
               fraction = (point - p_prev) / (p_next - p_prev)
"""

import math

# --- GDD formula (from gdd.py lines 582-584) ---
BASE_TEMP_C = 10
INTERVALS_PER_DAY = 288  # 24*60/5


def gdd_increment(tempf):
    """Calculate a single 5-minute GDD increment from Fahrenheit temp."""
    temp_c = (tempf - 32) * 5 / 9
    return max(0.0, (temp_c - BASE_TEMP_C)) / INTERVALS_PER_DAY


def chill_hours(readings_tempf, threshold=45):
    """Calculate chill hours from a list of 5-minute temp readings (°F)."""
    count = sum(1 for t in readings_tempf if t < threshold)
    return count * (5 / 60)


def linear_regression(data_points):
    """Simple linear regression on (x, y) pairs. Returns (slope, intercept)."""
    n = len(data_points)
    if n < 2:
        return 0, 0
    mean_x = sum(x for x, _ in data_points) / n
    mean_y = sum(y for _, y in data_points) / n
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in data_points)
    denominator = sum((x - mean_x) ** 2 for x, y in data_points)
    slope = numerator / denominator if denominator != 0 else 0
    intercept = mean_y - slope * mean_x
    return slope, intercept


def interpolate(p_prev, temp_prev, p_next, temp_next, point):
    """Linear interpolation between two temperature readings."""
    fraction = (point - p_prev) / (p_next - p_prev)
    return temp_prev + fraction * (temp_next - temp_prev)


# --- Tests ---

class TestGDDIncrement:
    def test_gdd_increment_above_base(self):
        """80°F = 26.67°C; increment = (26.67 - 10) / 288 ≈ 0.0579"""
        inc = gdd_increment(80)
        expected = ((80 - 32) * 5 / 9 - 10) / 288
        assert inc == pytest.approx(expected)
        assert inc > 0

    def test_gdd_increment_at_base(self):
        """50°F = 10°C exactly → increment = 0"""
        inc = gdd_increment(50)
        assert inc == 0.0

    def test_gdd_increment_below_base(self):
        """40°F = 4.44°C → below base → clamped to 0"""
        inc = gdd_increment(40)
        assert inc == 0.0

    def test_gdd_increment_extreme_heat(self):
        """110°F = 43.33°C → large but finite increment"""
        inc = gdd_increment(110)
        expected = ((110 - 32) * 5 / 9 - 10) / 288
        assert inc == pytest.approx(expected)
        assert inc > 0
        assert math.isfinite(inc)

    def test_gdd_cumulative_monotonic(self):
        """A series of GDD increments should produce a monotonically increasing cumulative sum."""
        temps = [45, 50, 55, 60, 65, 70, 75, 80]
        cumulative = 0
        prev = 0
        for t in temps:
            cumulative += gdd_increment(t)
            assert cumulative >= prev
            prev = cumulative
        # Final value should be positive (some temps are above base)
        assert cumulative > 0


class TestChillHours:
    def test_chill_hours_below_threshold(self):
        """12 readings below 45°F → 12 * 5/60 = 1.0 chill hours"""
        readings = [30, 35, 40, 42, 44, 38, 32, 41, 43, 39, 37, 36]
        result = chill_hours(readings)
        assert result == pytest.approx(1.0)

    def test_chill_hours_above_threshold(self):
        """All readings above 45°F → 0 chill hours"""
        readings = [50, 55, 60, 65, 70, 75]
        result = chill_hours(readings)
        assert result == 0.0


class TestLinearRegression:
    def test_linear_regression_two_points(self):
        """Two-point regression should give exact slope and intercept."""
        data = [(2020, 100), (2024, 108)]
        slope, intercept = linear_regression(data)
        assert slope == pytest.approx(2.0)
        assert intercept == pytest.approx(100 - 2.0 * 2020)

    def test_linear_regression_trend(self):
        """Known data → expected predicted DOY for a target year."""
        data = [(2020, 100), (2021, 102), (2022, 99), (2023, 101), (2024, 103)]
        slope, intercept = linear_regression(data)
        predicted_2025 = slope * 2025 + intercept
        # With a slight positive trend, prediction should be near the mean (~101)
        assert 95 < predicted_2025 < 110

    def test_doy_clamping(self):
        """Predicted DOY should be clamped to [1, 366]."""
        # Use a regression that would produce a very large DOY
        data = [(2000, 360), (2001, 365)]
        slope, intercept = linear_regression(data)
        predicted = slope * 2025 + intercept
        # Apply the clamping from gdd.py line 1057
        clamped = int(max(1.0, min(366.0, predicted)))
        assert 1 <= clamped <= 366


class TestInterpolation:
    def test_interpolation_midpoint(self):
        """Midpoint between two temps should be the average."""
        result = interpolate(0, 60.0, 100, 80.0, 50)
        assert result == pytest.approx(70.0)


# Need pytest for approx
import pytest
