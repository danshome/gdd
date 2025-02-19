import configparser
import os
import sqlite3
import sys
import time
from io import StringIO
import requests
import csv
from datetime import datetime, timedelta, timezone

# --- Constants for File Names ---
CONFIG_FILE = "config.ini"
GRAPEVINE_CSV = "grapevine_gdd.csv"
SUNSPOT_CSV = "SN_d_tot_V2.0.csv"

# --- Helper Functions for Logging ---
def log(message: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{now}] {message}")

def log_debug(message: str) -> None:
    if DEBUG:
        log("[DEBUG] " + message)

# === Function to Reload Configuration from config.ini ===
def reload_config() -> None:
    global DB_FILENAME, RETRY_SLEEP_TIME, RATE_LIMIT_DELAY, API_CALL_DELAY, DEBUG, RECALC_INTERVAL
    global MAC_ADDRESS, API_KEY, APPLICATION_KEY, BACKUP_MAC_ADDRESS, URL_TEMPLATE, START_DATE, CURRENT_DATE
    global OPENMETEO_LAT, OPENMETEO_LON, BUD_BREAK_START
    global GRAPEVINE_CSV, SUNSPOT_CSV

    # Check if config.ini exists
    if not os.path.exists(CONFIG_FILE):
        log(f"Error: {CONFIG_FILE} not found.")
        sys.exit(1)

    config = configparser.ConfigParser()
    try:
        config.read(CONFIG_FILE)
    except configparser.Error as e:
        log(f"Error reading {CONFIG_FILE}: {e}")
        sys.exit(1)

    # Global configuration
    try:
        raw_db_filename = config.get('global', 'db_filename')
    except Exception as e:
        log(f"Missing 'db_filename' in [global]: {e}")
        sys.exit(1)
    # Force DB_FILENAME to reside in a safe directory.
    safe_dir = config.get('global', 'db_directory', fallback=os.getcwd())
    DB_FILENAME = os.path.join(safe_dir, os.path.basename(raw_db_filename))
    try:
        RETRY_SLEEP_TIME = config.getfloat('global', 'retry_sleep_time')
        RATE_LIMIT_DELAY = config.getfloat('global', 'rate_limit_delay')
        API_CALL_DELAY = config.getfloat('global', 'api_call_delay', fallback=1.0)
        DEBUG = config.getboolean('global', 'debug', fallback=False)
        RECALC_INTERVAL = config.getint('global', 'recalc_interval', fallback=300)
    except Exception as e:
        log(f"Error in global configuration: {e}")
        sys.exit(1)

    # Primary weather station configuration
    try:
        MAC_ADDRESS = config.get('primary', 'mac_address')
        API_KEY = config.get('primary', 'api_key')
        APPLICATION_KEY = config.get('primary', 'application_key')
    except Exception as e:
        log(f"Error in primary configuration: {e}")
        sys.exit(1)

    # Backup weather station configuration
    try:
        BACKUP_MAC_ADDRESS = config.get('backup', 'mac_address')
    except Exception as e:
        log(f"Error in backup configuration: {e}")
        sys.exit(1)

    # API endpoint template for Ambient Weather
    try:
        URL_TEMPLATE = config.get('api', 'url_template')
    except Exception as e:
        log(f"Error in API configuration: {e}")
        sys.exit(1)

    # Date configuration
    try:
        START_DATE = datetime.strptime(config.get('date', 'start_date'), "%Y-%m-%d")
    except ValueError:
        log("Error: Invalid date format in config.ini for start_date.")
        sys.exit(1)
    CURRENT_DATE = datetime.now(timezone.utc).date()

    # Read bud_break_start from config or default to January 1 of the current year
    try:
        BUD_BREAK_START = datetime.strptime(config.get('date', 'bud_break_start'), "%Y-%m-%d").date()
    except Exception:
        BUD_BREAK_START = datetime(datetime.now(timezone.utc).year, 1, 1).date()

    # Open-Meteo configuration (for fallback)
    try:
        OPENMETEO_LAT = config.getfloat('openmeteo', 'latitude')
        OPENMETEO_LON = config.getfloat('openmeteo', 'longitude')
    except Exception as e:
        log(f"Error in Open-Meteo configuration: {e}")
        sys.exit(1)

    # Optionally, move CSV filenames into config as well:
    GRAPEVINE_CSV = config.get('files', 'grapevine_csv', fallback=GRAPEVINE_CSV)
    SUNSPOT_CSV = config.get('files', 'sunspot_csv', fallback=SUNSPOT_CSV)

# === Initial Config Load ===
reload_config()

# === Fields to Store (Order Matters) ===
fields_order = [
    "dateutc", "date", "tempf", "humidity", "baromrelin", "baromabsin", "feelsLike",
    "dewPoint", "winddir", "windspeedmph", "windgustmph", "maxdailygust", "windgustdir",
    "winddir_avg2m", "windspdmph_avg2m", "winddir_avg10m", "windspdmph_avg10m",
    "hourlyrainin", "dailyrainin", "monthlyrainin", "yearlyrainin", "battin", "battout",
    "tempinf", "humidityin", "feelsLikein", "dewPointin", "lastRain", "passkey", "time", "loc"
]

# === Open SQLite Database and Create Tables ===
try:
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()
except Exception as e:
    log(f"Error connecting to database: {e}")
    sys.exit(1)

def execute_sql(statement, params=()):
    try:
        cursor.execute(statement, params)
    except sqlite3.Error as e:
        log(f"SQL error: {e} while executing: {statement} with params: {params}")
        conn.rollback()

# Create tables with exception handling.
try:
    execute_sql("""
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
    );
    """)
    conn.commit()
except Exception as e:
    log(f"Error creating 'readings' table: {e}")
    sys.exit(1)

# Create indexes.
execute_sql("CREATE INDEX IF NOT EXISTS idx_readings_day ON readings (substr(date, 1, 10));")
execute_sql("CREATE INDEX IF NOT EXISTS idx_gdd ON readings (gdd);")
execute_sql("CREATE INDEX IF NOT EXISTS idx_readings_year ON readings((substr(date, 1, 4)));")
execute_sql("CREATE INDEX IF NOT EXISTS idx_readings_month ON readings((cast(substr(date, 6, 2) as integer)));")

conn.commit()

# === Import CSV Data into grapevine_gdd Table ===
try:
    execute_sql("""
    CREATE TABLE IF NOT EXISTS grapevine_gdd (
        variety TEXT PRIMARY KEY,
        heat_summation INTEGER
    );
    """)
    conn.commit()
except Exception as e:
    log(f"Error creating 'grapevine_gdd' table: {e}")
    sys.exit(1)

# Instead of enforcing an exact header, we check required columns.
required_headers = ['variety', 'heat_summation']
try:
    with open(GRAPEVINE_CSV, "r", newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        try:
            variety_idx = header.index("variety")
            heat_idx = header.index("heat_summation")
        except ValueError as ve:
            log(f"Required column missing in {GRAPEVINE_CSV}: {ve}")
            sys.exit(1)
        for row in reader:
            if len(row) <= max(variety_idx, heat_idx):
                continue
            variety = row[variety_idx]
            try:
                heat_summation = int(row[heat_idx])
            except ValueError:
                heat_summation = None
            execute_sql("""
                INSERT OR REPLACE INTO grapevine_gdd (variety, heat_summation)
                VALUES (?, ?)
            """, (variety, heat_summation))
    conn.commit()
except Exception as e:
    log(f"Error processing {GRAPEVINE_CSV}: {e}")

# --- Create the vineyard_pests table ---
try:
    execute_sql("""
    CREATE TABLE IF NOT EXISTS vineyard_pests (
        pest TEXT PRIMARY KEY,
        heat_summation INTEGER
    );
    """)
    conn.commit()
except Exception as e:
    log(f"Error creating 'vineyard_pests' table: {e}")
    sys.exit(1)

# --- Create an index on the heat_summation column for faster queries if needed ---
execute_sql("CREATE INDEX IF NOT EXISTS idx_vineyard_pests_heat ON vineyard_pests(heat_summation);")
conn.commit()

# --- Populate the vineyard_pests table with sample data ---
# These sample values are in degree days (GDD base 10°C).
# They represent the approximate heat summation when each pest reaches the critical stage for pesticide application.
pest_data = [
    ("Grape leafhopper", 170),
    ("Two-spotted spider mite", 220),
    ("Vine mealybug", 210),
    ("Scale insect", 230),
    ("Grape thrips", 180),
    ("Grape berry moth", 200)
]

for pest, heat in pest_data:
    try:
        execute_sql("""
            INSERT OR REPLACE INTO vineyard_pests (pest, heat_summation)
            VALUES (?, ?)
        """, (pest, heat))
    except Exception as e:
        log(f"Error inserting pest data for {pest}: {e}")
conn.commit()
log("Vineyard pests table populated.")

# === Import SunSpot Count Data from CSV ===
try:
    execute_sql("""
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
    );
    """)
    conn.commit()

    execute_sql("CREATE INDEX IF NOT EXISTS idx_sunspots_date ON sunspots(date);")
    execute_sql("CREATE INDEX IF NOT EXISTS idx_sunspots_year ON sunspots((substr(date, 1, 4)));")
    execute_sql("CREATE INDEX IF NOT EXISTS idx_sunspots_month ON sunspots((cast(substr(date, 6, 2) as integer)));")
    conn.commit()
except Exception as e:
    log(f"Error creating 'sunspots' table: {e}")
    sys.exit(1)

sunspot_url = "https://www.sidc.be/SILSO/INFO/sndtotcsv.php?"
download_file = True
try:
    head_response = requests.head(sunspot_url)
    remote_last_modified = None
    if head_response.status_code == 200:
        remote_last_modified_str = head_response.headers.get("Last-Modified")
        if remote_last_modified_str:
            try:
                remote_last_modified = datetime.strptime(remote_last_modified_str, "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
            except Exception as ex:
                log(f"Error parsing remote Last-Modified header: {ex}")
    if remote_last_modified and os.path.exists(SUNSPOT_CSV):
        local_mod_timestamp = os.path.getmtime(SUNSPOT_CSV)
        local_mod_datetime = datetime.fromtimestamp(local_mod_timestamp, timezone.utc)
        if remote_last_modified <= local_mod_datetime:
            log("Sunspot CSV has not changed; skipping download.")
            download_file = False
except Exception as e:
    log(f"Error during sunspot CSV header check: {e}")
    download_file = True

if download_file:
    try:
        response = requests.get(sunspot_url)
        if response.status_code == 200:
            with open(SUNSPOT_CSV, "w", newline="") as f:
                f.write(response.text)
            log("Sunspot data updated from SIDC.")
        else:
            log(f"Failed to fetch sunspot data. HTTP Status Code: {response.status_code}")
    except Exception as e:
        log(f"Exception during sunspot CSV download: {e}")

try:
    with open(SUNSPOT_CSV, "r", newline="") as f:
        csv_data = f.read()
    csvfile = StringIO(csv_data)
    reader = csv.reader(csvfile, delimiter=";")
    header = next(reader)  # Skip header row
    for row in reader:
        if len(row) < 8:
            continue
        try:
            year = int(row[0])
        except Exception:
            continue
        if year < 2010:
            continue
        try:
            month = int(row[1])
            day = int(row[2])
        except Exception:
            continue
        try:
            fraction = float(row[3])
        except Exception:
            fraction = None
        try:
            daily_total = int(row[4])
            if daily_total == -1:
                daily_total = None
        except Exception:
            daily_total = None
        try:
            std_dev = float(row[5])
        except Exception:
            std_dev = None
        try:
            num_obs = int(row[6])
        except Exception:
            num_obs = None
        try:
            definitive = int(row[7])
        except Exception:
            definitive = None
        date_str = f"{year:04d}-{month:02d}-{day:02d}"
        execute_sql("""
            INSERT OR REPLACE INTO sunspots 
            (year, month, day, fraction, daily_total, std_dev, num_obs, definitive, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (year, month, day, fraction, daily_total, std_dev, num_obs, definitive, date_str))
    conn.commit()
    log("Sunspot CSV processed from local file.")
except Exception as e:
    log(f"Error processing {SUNSPOT_CSV}: {e}")

# === Function: Recalculate Cumulative, Hourly, and Daily GDD ===
def recalcGDD(full: bool = False) -> None:
    """
    Recalculates cumulative GDD values.
    Modes:
      - Incremental (full=False): For each year, only update rows after the last calculated value.
      - Full (full=True): For each year, start at the beginning and recalculate GDD for all rows.
    """
    if full:
        log("Performing full GDD recalculation from the beginning...")
    else:
        log("Performing incremental GDD recalculation...")
    try:
        cursor.execute("SELECT DISTINCT substr(date, 1, 4) as year FROM readings ORDER BY year ASC")
        years = [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        log(f"Error fetching distinct years: {e}")
        return

    for year in years:
        try:
            if full:
                cumulative_gdd = 0
                log(f"For year {year}, starting full recalculation from beginning with cumulative GDD {cumulative_gdd:.3f}.")
                cursor.execute("SELECT dateutc, tempf, date FROM readings WHERE substr(date, 1, 4)=? ORDER BY dateutc ASC", (year,))
            else:
                cursor.execute("SELECT MAX(dateutc), gdd FROM readings WHERE substr(date, 1, 4)=? AND gdd>0", (year,))
                result = cursor.fetchone()
                if result[0] is not None:
                    last_dateutc = result[0]
                    cumulative_gdd = result[1]
                    log(f"For year {year}, starting incremental recalculation from dateutc {last_dateutc} with cumulative GDD {cumulative_gdd:.3f}.")
                    cursor.execute("SELECT dateutc, tempf, date FROM readings WHERE substr(date, 1, 4)=? AND dateutc > ? ORDER BY dateutc ASC", (year, last_dateutc))
                else:
                    cumulative_gdd = 0
                    log(f"For year {year}, no previous GDD found. Recalculating from start.")
                    cursor.execute("SELECT dateutc, tempf, date FROM readings WHERE substr(date, 1, 4)=? ORDER BY dateutc ASC", (year,))
            rows = cursor.fetchall()
            for dateutc, tempf, date_str in rows:
                if tempf is None:
                    continue
                try:
                    val = float(tempf)
                except Exception as e:
                    log(f"Skipping record {dateutc} due to invalid tempf: {tempf}")
                    continue
                temp_c = (val - 32) * 5 / 9
                inc = max(0, (temp_c - base_temp_C)) / 288
                cumulative_gdd += inc
                execute_sql("UPDATE readings SET gdd = ? WHERE dateutc = ?", (cumulative_gdd, dateutc))
        except sqlite3.Error as e:
            log(f"Error during GDD recalculation for year {year}: {e}")
    conn.commit()
    if full:
        log("Full GDD recalculation complete.")
    else:
        log("Incremental GDD recalculation complete.")

# === Fetch Data from Ambient Weather API ===
def fetch_day_data(mac_address: str, end_date: str):
    """
    Fetch data for a given day using Ambient Weather’s API.
    Sleeps for API_CALL_DELAY seconds before each call.
    Returns the JSON response or None on errors.
    """
    url = URL_TEMPLATE.format(
        mac_address=mac_address,
        api_key=API_KEY,
        application_key=APPLICATION_KEY,
        end_date=end_date
    )
    log(f"Calling API URL: {url}")
    while True:
        log_debug(f"Sleeping for API_CALL_DELAY of {API_CALL_DELAY} seconds before API call.")
        time.sleep(API_CALL_DELAY)
        try:
            response = requests.get(url, timeout=10)
        except Exception as ex:
            log(f"Request error for URL {url}: {ex}. Retrying in {RETRY_SLEEP_TIME} seconds...")
            time.sleep(RETRY_SLEEP_TIME)
            continue

        if response.status_code == 404:
            log(f"HTTP error 404 for URL {url}: Not Found. No data available.")
            return None

        if response.status_code in [401, 403]:
            log(f"HTTP error {response.status_code} for URL {url}: Unauthorized or forbidden. Not retrying.")
            return None

        if response.status_code == 503:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = int(retry_after)
                except ValueError:
                    from email.utils import parsedate_to_datetime
                    try:
                        dt_retry = parsedate_to_datetime(retry_after)
                        delay = (dt_retry - datetime.now(timezone.utc)).total_seconds()
                    except Exception as ex:
                        log(f"Error parsing Retry-After header '{retry_after}': {ex}. Retrying in {RETRY_SLEEP_TIME} seconds...")
                        delay = RETRY_SLEEP_TIME
                if delay > 30:
                    log(f"Received 503 for URL {url} with Retry-After delay {delay:.0f} sec (>30 sec); not retrying.")
                    return None
                else:
                    log(f"Received 503 for URL {url}. Sleeping for {delay:.0f} sec before retrying...")
                    time.sleep(delay)
                    continue
            else:
                log(f"Received 503 for URL {url} with no Retry-After header; retrying in {RETRY_SLEEP_TIME} seconds...")
                time.sleep(RETRY_SLEEP_TIME)
                continue

        if response.status_code == 429:
            log(f"Received 429 Too Many Requests for URL {url}. Sleeping for {RETRY_SLEEP_TIME} seconds before retrying...")
            time.sleep(RETRY_SLEEP_TIME)
            continue

        if response.status_code >= 500:
            log(f"Server error {response.status_code} for URL {url}. Retrying in {RETRY_SLEEP_TIME} seconds...")
            time.sleep(RETRY_SLEEP_TIME)
            continue

        try:
            response.raise_for_status()
        except Exception as ex:
            log(f"HTTP error {response.status_code} for URL {url}: {ex}. Retrying in {RETRY_SLEEP_TIME} seconds...")
            time.sleep(RETRY_SLEEP_TIME)
            continue

        return response.json()

# === Fetch Data from Open-Meteo API as Fallback ===
def fetch_openmeteo_data(day_str: str):
    """
    Fetch hourly data for the given day from the Open-Meteo archive.
    Returns a pandas DataFrame with columns 'date' and 'tempf'.
    """
    try:
        import openmeteo_requests
        import requests_cache
        from retry_requests import retry
        import pandas as pd
    except ImportError:
        log("Open-Meteo packages not installed. Please install openmeteo_requests, requests_cache, and retry_requests.")
        return None

    start_date = day_str
    dt = datetime.strptime(day_str, "%Y-%m-%d")
    end_date = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": OPENMETEO_LAT,
        "longitude": OPENMETEO_LON,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch"
    }
    log(f"Calling Open-Meteo API URL: {url} with params: {params}")
    responses = openmeteo.weather_api(url, params=params)
    if responses:
        response = responses[0]
        hourly = response.Hourly()
        import pandas as pd
        time_range = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )
        temp_values = hourly.Variables(0).ValuesAsNumpy()
        df = pd.DataFrame({"date": time_range, "tempf": temp_values})
        return df
    else:
        log("No Open-Meteo data received.")
        return None

# === Fetch Data from Open-Meteo API for Forecast (NEW) ===
def fetch_openmeteo_forecast():
    """
    Fetch a 14-day hourly forecast from Open-Meteo.
    Returns a pandas DataFrame with columns:
      - date (Timestamp)
      - temperature_2m (in Fahrenheit, as requested)
    """
    try:
        import openmeteo_requests
        import requests_cache
        from retry_requests import retry
        import pandas as pd
    except ImportError:
        log("Open-Meteo packages not installed. Please install openmeteo_requests, requests_cache, and retry_requests.")
        return None

    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": OPENMETEO_LAT,
        "longitude": OPENMETEO_LON,
        "hourly": "temperature_2m",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": "America/Chicago",
        "models": "ecmwf_aifs025",
        "forecast_days": 15
    }
    log(f"Fetching hourly forecast data from Open-Meteo with params: {params}")
    responses = openmeteo.weather_api(url, params=params)
    if responses:
        response = responses[0]
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        import pandas as pd
        date_range = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )
        df = pd.DataFrame({"date": date_range, "temperature_2m": hourly_temperature_2m})
        log("Hourly forecast data successfully retrieved from Open-Meteo.")
        return df
    else:
        log("No forecast data received from Open-Meteo.")
        return None

# === Base Temperatures for GDD Calculations ===
base_temp_C = 10  # For 5-minute cumulative GDD (°C)
base_temp_F = 50  # For hourly/daily calculations (°F)

# === Gap Filling via Interpolation Function ===
def fill_missing_data_by_gap(day_str: str) -> None:
    """
    For a given day (YYYY-MM-DD), this function snaps each reading’s timestamp to the expected 5-minute grid,
    then fills in any missing intervals by linearly interpolating temperature values.
    Interpolated tempf values are rounded to one decimal.
    Inserted rows get is_generated = 1 and mac_source = "INTERP".
    """
    try:
        dt_day = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception as e:
        log(f"Error parsing day_str {day_str}: {e}")
        return
    expected_start = int(dt_day.timestamp())
    expected_end = expected_start + (287 * 300)
    expected_points = list(range(expected_start, expected_end + 1, 300))
    try:
        cursor.execute(
            "SELECT dateutc, tempf FROM readings WHERE substr(date, 1, 10)=? ORDER BY dateutc ASC",
            (day_str,)
        )
        rows = cursor.fetchall()
    except sqlite3.Error as e:
        log(f"Error fetching readings for {day_str}: {e}")
        return
    if not rows:
        log(f"No readings found for {day_str} to interpolate.")
        return

    tolerance = 10  # seconds tolerance for snapping
    # Build a dictionary of available data keyed by the snapped grid timestamp.
    available = {}
    for ts, temp in rows:
        snapped = expected_start + ((ts - expected_start) // 300) * 300
        # Only add the first reading found for this grid point.
        if snapped not in available:
            available[snapped] = temp

    grid_points = sorted(available.keys())
    # Iterate over the complete grid and insert interpolated readings where data is missing.
    for point in expected_points:
        if point in available:
            continue  # already have a reading for this grid point
        # Find the nearest available points before and after the missing point.
        prev_points = [p for p in grid_points if p < point]
        next_points = [p for p in grid_points if p > point]
        if prev_points and next_points:
            p_prev = max(prev_points)
            p_next = min(next_points)
            temp_prev = available[p_prev]
            temp_next = available[p_next]
            fraction = (point - p_prev) / (p_next - p_prev)
            interp_temp = round(temp_prev + fraction * (temp_next - temp_prev), 1)
        elif prev_points:
            interp_temp = available[max(prev_points)]
        elif next_points:
            interp_temp = available[min(next_points)]
        else:
            continue  # should not occur
        dt_new = datetime.fromtimestamp(point, tz=timezone.utc)
        new_date_str = dt_new.isoformat() + "Z"
        execute_sql("""
                INSERT OR REPLACE INTO readings
                (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                VALUES (?, ?, ?, 0, 0, 0, 1, "INTERP")
        """, (point, new_date_str, interp_temp))
        log(f"Interpolated reading for {new_date_str}: tempf {interp_temp:.1f}")
    conn.commit()

##############################
# FORECAST INTEGRATION SECTION
##############################
def append_forecast_data() -> None:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        execute_sql("DELETE FROM readings WHERE substr(date, 1, 10) >= ?", (today_str,))
        conn.commit()
    except sqlite3.Error as e:
        log(f"Error deleting forecast data: {e}")
    log(f"Deleted existing forecast data from readings table (rows with date >= {today_str}).")
    forecast_df = fetch_openmeteo_forecast()
    if forecast_df is not None and not forecast_df.empty:
        import pandas as pd
        for idx, row in forecast_df.iterrows():
            dt_forecast = row["date"]
            ts = int(dt_forecast.timestamp())
            forecast_date_str = dt_forecast.isoformat() + "Z"
            tempf = row["temperature_2m"]
            try:
                execute_sql("""
                    INSERT OR REPLACE INTO readings
                    (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                    VALUES (?, ?, ?, 0, 0, 0, 1, ?)
                """, (ts, forecast_date_str, tempf, "OPENMETEO"))
            except Exception as ex:
                log(f"Error inserting forecast reading for {forecast_date_str}: {ex}")
        conn.commit()
        log(f"Inserted forecast data for {len(forecast_df)} hours into readings.")
    else:
        log("Forecast data unavailable from Open-Meteo.")

# --- NEW: Project Bud Break Using Historical Regression ---
def project_bud_break_regression() -> None:
    """
    For each grape variety in grapevine_gdd, use historical bud break dates to derive a trend.
    For each year from 2012 until the previous year, find the first date when cumulative GDD (from January 1)
    reaches the heat_summation threshold for that variety. Convert that date to day-of-year (DOY),
    perform a linear regression (DOY vs. year), and use the trend to predict the current year's bud break.
    Update grapevine_gdd with the predicted bud break date.
    """
    try:
        # Try to add the new column; if it already exists, log and continue.
        execute_sql("ALTER TABLE grapevine_gdd ADD COLUMN projected_bud_break TEXT")
        conn.commit()
        log("Added column projected_bud_break to grapevine_gdd.")
    except sqlite3.OperationalError as e:
        if "duplicate column name: projected_bud_break" in str(e):
            log("Column projected_bud_break already exists, skipping addition.")
        else:
            log(f"Error adding column projected_bud_break: {e}")

    current_year = datetime.now(timezone.utc).year
    historical_years = list(range(2012, current_year))  # use past years
    try:
        cursor.execute("SELECT variety, heat_summation FROM grapevine_gdd")
        varieties = cursor.fetchall()
    except sqlite3.Error as e:
        log(f"Error fetching grapevine_gdd data: {e}")
        return

    for variety, heat_sum in varieties:
        if heat_sum is None:
            log(f"Skipping {variety} due to undefined heat_summation.")
            continue
        data_points = []  # list of (year, bud_break_doy)
        for yr in historical_years:
            try:
                cursor.execute(
                    "SELECT date FROM readings WHERE substr(date, 1, 4)=? AND gdd >= ? ORDER BY date ASC LIMIT 1",
                    (str(yr), heat_sum)
                )
            except sqlite3.Error as e:
                log(f"Error fetching readings for year {yr}: {e}")
                continue
            row = cursor.fetchone()
            if row:
                try:
                    dt = datetime.fromisoformat(row[0].rstrip("Z"))
                    bud_break_doy = dt.timetuple().tm_yday
                    data_points.append((yr, bud_break_doy))
                except Exception as ex:
                    log(f"Error parsing date {row[0]} for {variety} in year {yr}: {ex}")
        if len(data_points) < 2:
            log(f"Not enough historical data for {variety} to perform regression; skipping.")
            continue
        # Simple linear regression: slope = cov(x, y)/var(x), intercept = mean(y) - slope * mean(x)
        mean_year = sum(x for x, _ in data_points) / len(data_points)
        mean_doy = sum(y for _, y in data_points) / len(data_points)
        numerator = sum((x - mean_year) * (y - mean_doy) for x, y in data_points)
        denominator = sum((x - mean_year) ** 2 for x, y in data_points)
        slope = numerator / denominator if denominator != 0 else 0
        intercept = mean_doy - slope * mean_year
        predicted_doy = slope * current_year + intercept
        predicted_doy = max(1, min(366, predicted_doy))
        predicted_date = (datetime(current_year, 1, 1) + timedelta(days=predicted_doy - 1)).date()
        execute_sql("""
            UPDATE grapevine_gdd
            SET projected_bud_break = ?
            WHERE variety = ?
        """, (predicted_date.isoformat(), variety))
        log(f"Predicted bud break for {variety} using regression: {predicted_date.isoformat()} (slope: {slope:.2f}, intercept: {intercept:.2f})")
    conn.commit()

##############################
# Main Historical Data Ingestion Loop
##############################
lastRecalcRowCount = 0
new_total = 0
day = START_DATE.date()

while day < CURRENT_DATE:
    reload_config()
    day_str = day.strftime("%Y-%m-%d")
    try:
        cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=?", (day_str,))
        old_count = cursor.fetchone()[0]
    except sqlite3.Error as e:
        log(f"Error fetching count for {day_str}: {e}")
        old_count = 0

    # --- Primary Data Insertion ---
    if old_count >= 287:
        log(f"{day_str}: Already has {old_count} readings; skipping primary API call.")
    else:
        next_day = day + timedelta(days=1)
        next_day_str = next_day.strftime("%Y-%m-%d")
        log(f"Fetching primary data for {day_str} (endDate={next_day_str}); current DB count = {old_count}")
        primary_data = fetch_day_data(MAC_ADDRESS, next_day_str)
        if not primary_data:
            log(f"Warning: No primary data received for {day_str}.")
        else:
            for reading in primary_data:
                raw_date = reading.get("date")
                if not raw_date:
                    continue
                try:
                    dt = datetime.fromisoformat(raw_date.rstrip("Z")).replace(tzinfo=timezone.utc)
                except Exception as ex:
                    log(f"Error parsing date '{raw_date}': {ex}")
                    continue
                if dt.date() != day:
                    continue
                values = {key: reading.get(key, None) for key in fields_order}
                ts = int(dt.timestamp())
                values["dateutc"] = ts
                values["date"] = dt.isoformat() + "Z"
                tempf = values.get("tempf")
                try:
                    numeric_tempf = float(tempf) if tempf is not None else None
                except Exception as e:
                    log(f"Skipping reading at {values.get('date')} due to invalid tempf: {tempf}")
                    continue
                if numeric_tempf is None:
                    log(f"Skipping reading at {values.get('date')} due to missing tempf.")
                    continue
                sql = f"""
                    INSERT OR IGNORE INTO readings
                    ({', '.join(fields_order)}, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                    VALUES ({', '.join(['?'] * len(fields_order))}, ?, ?, ?, ?, ?)
                """
                try:
                    insert_tuple = tuple(values.get(k, None) for k in fields_order) + (0, 0, 0, 0, MAC_ADDRESS)
                    cursor.execute(sql, insert_tuple)
                except Exception as ex:
                    log(f"Error inserting primary reading for {values.get('date')}: {ex}")
                    continue
            conn.commit()
            try:
                cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=?", (day_str,))
                new_count = cursor.fetchone()[0]
                inserted_count = new_count - old_count
                new_total += inserted_count
                log(f"Inserted {inserted_count} new primary readings for {day_str} (total now: {new_count}).")
            except sqlite3.Error as e:
                log(f"Error fetching new count for {day_str}: {e}")

    # --- Backup Data Insertion ---
    try:
        cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=? AND tempf IS NOT NULL", (day_str,))
        valid_count = cursor.fetchone()[0]
    except sqlite3.Error as e:
        log(f"Error fetching valid count for {day_str}: {e}")
        valid_count = 0
    if valid_count < 287:
        log(f"{day_str}: Only {valid_count} valid readings from primary. Attempting to use backup data.")
        next_day = day + timedelta(days=1)
        next_day_str = next_day.strftime("%Y-%m-%d")
        backup_data = fetch_day_data(BACKUP_MAC_ADDRESS, next_day_str)
        if backup_data:
            for backup_reading in backup_data:
                raw_date = backup_reading.get("date")
                if not raw_date:
                    continue
                try:
                    dt = datetime.fromisoformat(raw_date.rstrip("Z")).replace(tzinfo=timezone.utc)
                except Exception as ex:
                    log(f"Error parsing backup date '{raw_date}': {ex}")
                    continue
                if dt.date() != day:
                    continue
                ts = int(dt.timestamp())
                backup_values = {key: backup_reading.get(key, None) for key in fields_order}
                backup_values["dateutc"] = ts
                backup_values["date"] = dt.isoformat() + "Z"
                try:
                    cursor.execute("SELECT tempf FROM readings WHERE dateutc = ?", (ts,))
                    existing = cursor.fetchone()
                except sqlite3.Error as e:
                    log(f"Error checking existing backup reading for {raw_date}: {e}")
                    continue
                if existing is None:
                    sql = f"""
                        INSERT OR IGNORE INTO readings
                        ({', '.join(fields_order)}, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                        VALUES ({', '.join(['?'] * len(fields_order))}, ?, ?, ?, ?, ?)
                    """
                    try:
                        insert_tuple = tuple(backup_values.get(k, None) for k in fields_order) + (0, 0, 0, 0, BACKUP_MAC_ADDRESS)
                        cursor.execute(sql, insert_tuple)
                        log(f"Inserted backup reading for {raw_date} from backup station.")
                    except Exception as ex:
                        log(f"Error inserting backup reading for {raw_date}: {ex}")
                else:
                    if existing[0] is None:
                        update_clause = ", ".join([f"{col} = ?" for col in fields_order])
                        update_values = [backup_values.get(col, None) for col in fields_order]
                        try:
                            cursor.execute(
                                f"UPDATE readings SET {update_clause}, is_generated = ?, mac_source = ? WHERE dateutc = ?",
                                (*update_values, 0, BACKUP_MAC_ADDRESS, ts)
                            )
                            log(f"Updated backup reading for {raw_date} from backup station.")
                        except Exception as ex:
                            log(f"Error updating backup reading for {raw_date}: {ex}")
            conn.commit()
            try:
                cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=? AND tempf IS NOT NULL", (day_str,))
                valid_count = cursor.fetchone()[0]
            except sqlite3.Error as e:
                log(f"Error fetching valid count after backup for {day_str}: {e}")
        else:
            log(f"No backup data received for {day_str}.")

    # --- Open-Meteo Fallback ---
    if valid_count < 287:
        log(f"{day_str}: Only {valid_count} valid readings after backup. Attempting to use Open-Meteo data.")
        df_obj = fetch_openmeteo_data(day_str)
        if df_obj is not None:
            import pandas as pd
            df = df_obj
            if not df.empty:
                for idx, row in df.iterrows():
                    import pandas as pd
                    if pd.isnull(row['tempf']):
                        log(f"Skipping row {idx} due to missing tempf value.")
                        continue
                    try:
                        tempf = round(float(row['tempf']), 1)
                    except Exception as ex:
                        log(f"Error rounding tempf value for row {idx}: {ex}")
                        continue
                    ts = int(row['date'].timestamp())
                    date_str = row['date'].isoformat() + "Z"
                    sql = """
                        INSERT OR IGNORE INTO readings
                        (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                        VALUES (?, ?, ?, 0, 0, 0, ?, ?)
                    """
                    try:
                        cursor.execute(sql, (ts, date_str, tempf, 1, "OPENMETEO"))
                    except Exception as ex:
                        log(f"Error inserting Open-Meteo reading for {date_str}: {ex}")
                conn.commit()
                try:
                    cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=? AND tempf IS NOT NULL", (day_str,))
                    valid_count = cursor.fetchone()[0]
                    log(f"After Open-Meteo fallback, valid readings for {day_str}: {valid_count}")
                except sqlite3.Error as e:
                    log(f"Error fetching valid count after Open-Meteo for {day_str}: {e}")
            else:
                log(f"No Open-Meteo data received for {day_str}.")
        else:
            log(f"No Open-Meteo data received for {day_str}.")

    # --- Interpolation as Last Resort ---
    if valid_count < 288:
        log(f"{day_str}: Only {valid_count} valid readings after all fallbacks. Filling gaps via interpolation.")
        fill_missing_data_by_gap(day_str)
    else:
        log(f"{day_str}: All intervals have valid temperature data.")
    try:
        cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date, 1, 10)=?", (day_str,))
        final_day_count = cursor.fetchone()[0]
        log(f"After interpolation, {day_str} has {final_day_count} readings.")
    except sqlite3.Error as e:
        log(f"Error fetching final count for {day_str}: {e}")
    try:
        cursor.execute("SELECT COUNT(*) FROM readings")
        totalRowCount = cursor.fetchone()[0]
    except sqlite3.Error as e:
        log(f"Error fetching total row count: {e}")
        totalRowCount = 0
    if totalRowCount - lastRecalcRowCount >= RECALC_INTERVAL:
        recalcGDD()
        lastRecalcRowCount = totalRowCount
    day = day + timedelta(days=1)

##############################
# Append Forecast Data
##############################
append_forecast_data()

# --- Final Recalculation of GDD ---
log("Clearing all GDD values before full recalculation...")
execute_sql("UPDATE readings SET gdd = NULL")
conn.commit()

log("Performing final full recalculation of cumulative, hourly, and daily GDD...")
recalcGDD(full=True)
conn.commit()

# --- NEW: Project Bud Break Using Historical Regression ---
project_bud_break_regression()

conn.close()
log("Data retrieval complete. Added " + str(new_total) + " new primary readings in total.")