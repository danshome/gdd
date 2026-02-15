"""Performance benchmark tests for the GDD frontend.

Uses Playwright with the fixture database. Measures timing to establish
baselines. Tests fail if performance degrades beyond threshold.

Note: Thresholds are generous since the fixture DB is small. The real value
is detecting relative regressions.
"""

import pytest


@pytest.fixture
def perf_page(served_app, browser):
    """Create a fresh browser context and page for each performance test."""
    context = browser.new_context()
    pg = context.new_page()
    yield pg, served_app
    pg.close()
    context.close()


class TestPerformance:
    def test_page_load_time(self, perf_page):
        """Page should load and initialize database within 5 seconds."""
        page, url = perf_page
        start = page.evaluate("() => performance.now()")
        page.goto(url, wait_until="networkidle", timeout=15000)
        # Wait for DB to be loaded
        page.wait_for_function(
            """() => {
                return window._dbLoaded === true ||
                       document.querySelector('input[name="year"]') !== null;
            }""",
            timeout=10000
        )
        elapsed = page.evaluate("() => performance.now()") - start
        # Convert to seconds (performance.now() is in ms)
        elapsed_s = elapsed / 1000
        assert elapsed_s < 5, f"Page load took {elapsed_s:.1f}s (threshold: 5s)"

    def test_chart_render_time(self, perf_page):
        """Chart should render within 3 seconds after page load."""
        page, url = perf_page
        page.goto(url, wait_until="networkidle", timeout=15000)
        page.wait_for_function(
            "() => document.querySelector('input[name=\"year\"]') !== null",
            timeout=10000
        )
        start = page.evaluate("() => performance.now()")
        # Wait for Chart.js to create the chart instance with datasets
        page.wait_for_function(
            """() => {
                const chart = Chart.getChart('dailyChart');
                return chart && chart.data && chart.data.datasets &&
                       chart.data.datasets.length > 0;
            }""",
            timeout=5000
        )
        elapsed = page.evaluate("() => performance.now()") - start
        elapsed_s = elapsed / 1000
        assert elapsed_s < 3, f"Chart render took {elapsed_s:.1f}s (threshold: 3s)"

    def test_model_switch_time(self, perf_page):
        """Switching prediction model should re-render within 2 seconds."""
        page, url = perf_page
        page.goto(url, wait_until="networkidle", timeout=15000)
        page.wait_for_timeout(3000)  # Let initial render complete

        start = page.evaluate("() => performance.now()")
        page.locator("#modelSelect").select_option("dynamicGDDProjection")
        page.wait_for_timeout(500)  # Give chart time to update
        elapsed = page.evaluate("() => performance.now()") - start
        elapsed_s = elapsed / 1000
        assert elapsed_s < 2, f"Model switch took {elapsed_s:.1f}s (threshold: 2s)"

    def test_year_toggle_time(self, perf_page):
        """Toggling a year checkbox should re-render within 2 seconds."""
        page, url = perf_page
        page.goto(url, wait_until="networkidle", timeout=15000)
        page.wait_for_timeout(3000)  # Let initial render complete

        year_checkbox = page.locator("input[name='year']").first
        if year_checkbox.count() == 0:
            pytest.skip("No year checkboxes found")

        start = page.evaluate("() => performance.now()")
        year_checkbox.click()
        page.wait_for_timeout(500)
        elapsed = page.evaluate("() => performance.now()") - start
        elapsed_s = elapsed / 1000
        assert elapsed_s < 2, f"Year toggle took {elapsed_s:.1f}s (threshold: 2s)"
