import pytest
import sqlite3
import os
import subprocess
import signal
import time
import shutil

FIXTURE_DB = os.path.join(os.path.dirname(__file__), "fixture.sqlite")
REAL_DB = os.path.join(os.path.dirname(__file__), "..", "ambient_weather.sqlite")
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")


@pytest.fixture
def real_db():
    """Read-only connection to the production database."""
    if not os.path.exists(REAL_DB):
        pytest.skip("Real database not available")
    conn = sqlite3.connect(f"file:{REAL_DB}?mode=ro", uri=True)
    yield conn
    conn.close()


@pytest.fixture
def fixture_db():
    """Read-only connection to the small test fixture database."""
    if not os.path.exists(FIXTURE_DB):
        pytest.skip("Fixture database not available — run: python tests/create_fixture_db.py")
    conn = sqlite3.connect(f"file:{FIXTURE_DB}?mode=ro", uri=True)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def served_app(tmp_path_factory):
    """Copy frontend files + fixture DB to temp dir, serve via HTTP."""
    if not os.path.exists(FIXTURE_DB):
        pytest.skip("Fixture database not available — run: python tests/create_fixture_db.py")

    tmp_dir = tmp_path_factory.mktemp("frontend")

    # Copy frontend files
    for filename in ["index.html", "gdd.js", "styles.css"]:
        src = os.path.join(PROJECT_ROOT, filename)
        if os.path.exists(src):
            shutil.copy2(src, tmp_dir / filename)

    # Copy fixture DB as ambient_weather.sqlite (the name the frontend expects)
    shutil.copy2(FIXTURE_DB, tmp_dir / "ambient_weather.sqlite")

    # Find an available port
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()

    proc = subprocess.Popen(
        ["python", "-m", "http.server", str(port), "--directory", str(tmp_dir)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    time.sleep(1)  # wait for server startup
    yield f"http://localhost:{port}"
    proc.send_signal(signal.SIGTERM)
    proc.wait()
