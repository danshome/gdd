import configparser
import os
import sqlite3
import time
from io import StringIO

import requests
import csv
from datetime import datetime, timedelta, timezone


# --- Helper Functions for Logging ---
def log(message):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"[{now}] {message}")


def log_debug(message):
    if DEBUG:
        log("[DEBUG] " + message)


# === Function to Reload Configuration from config.ini ===
def reload_config():
    global DB_FILENAME, RETRY_SLEEP_TIME, RATE_LIMIT_DELAY, API_CALL_DELAY, DEBUG, RECALC_INTERVAL
    global MAC_ADDRESS, API_KEY, APPLICATION_KEY, BACKUP_MAC_ADDRESS, URL_TEMPLATE, START_DATE, CURRENT_DATE
    global OPENMETEO_LAT, OPENMETEO_LON

    config = configparser.ConfigParser()
    config.read('config.ini')

    # Global configuration
    DB_FILENAME = config.get('global', 'db_filename')
    RETRY_SLEEP_TIME = config.getfloat('global', 'retry_sleep_time')
    RATE_LIMIT_DELAY = config.getfloat('global', 'rate_limit_delay')
    API_CALL_DELAY = config.getfloat('global', 'api_call_delay', fallback=1.0)
    DEBUG = config.getboolean('global', 'debug')
    RECALC_INTERVAL = config.getint('global', 'recalc_interval', fallback=100)

    # Primary weather station configuration
    MAC_ADDRESS = config.get('primary', 'mac_address')
    API_KEY = config.get('primary', 'api_key')
    APPLICATION_KEY = config.get('primary', 'application_key')

    # Backup weather station configuration
    BACKUP_MAC_ADDRESS = config.get('backup', 'mac_address')

    # API endpoint template for Ambient Weather
    URL_TEMPLATE = config.get('api', 'url_template')

    # Date configuration
    START_DATE = datetime.strptime(config.get('date', 'start_date'), "%Y-%m-%d")
    CURRENT_DATE = datetime.now(timezone.utc).date()

    # Open-Meteo configuration (for fallback)
    OPENMETEO_LAT = config.getfloat('openmeteo', 'latitude')
    OPENMETEO_LON = config.getfloat('openmeteo', 'longitude')


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
conn = sqlite3.connect(DB_FILENAME)
cursor = conn.cursor()

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
);
""")
conn.commit()

cursor.execute("CREATE INDEX IF NOT EXISTS idx_readings_day ON readings (substr(date, 1, 10));")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_gdd ON readings (gdd);")
conn.commit()

# === Import CSV Data into a New Table ===
cursor.execute("""
CREATE TABLE IF NOT EXISTS grapevine_gdd (
    variety TEXT PRIMARY KEY,
    heat_summation INTEGER
);
""")
conn.commit()

with open("grapevine_gdd.csv", "r", newline='') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)  # Skip header row.
    for row in reader:
        variety, heat_summation = row
        try:
            heat_summation = int(heat_summation)
        except ValueError:
            heat_summation = None
        cursor.execute("""
            INSERT OR REPLACE INTO grapevine_gdd (variety, heat_summation)
            VALUES (?, ?)
        """, (variety, heat_summation))
conn.commit()

# === Import SunSpot Count Data from CSV ===
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
);
""")
conn.commit()

sunspot_url = "https://www.sidc.be/SILSO/INFO/sndtotcsv.php?"
local_file = "SN_d_tot_V2.0.csv"

# First, perform a HEAD request to get the remote file's Last-Modified header.
head_response = requests.head(sunspot_url)
remote_last_modified = None
if head_response.status_code == 200:
    remote_last_modified_str = head_response.headers.get("Last-Modified")
    if remote_last_modified_str:
        try:
            # Use timezone-aware datetime
            remote_last_modified = datetime.strptime(remote_last_modified_str, "%a, %d %b %Y %H:%M:%S GMT").replace(
                tzinfo=timezone.utc)
        except Exception as ex:
            log(f"Error parsing remote Last-Modified header: {ex}")

download_file = True
if remote_last_modified and os.path.exists(local_file):
    local_mod_timestamp = os.path.getmtime(local_file)
    local_mod_datetime = datetime.fromtimestamp(local_mod_timestamp, timezone.utc)
    if remote_last_modified <= local_mod_datetime:
        log("Sunspot CSV has not changed; skipping download.")
        download_file = False

if download_file:
    response = requests.get(sunspot_url)
    if response.status_code == 200:
        # Write the updated CSV to the local file.
        with open(local_file, "w", newline="") as f:
            f.write(response.text)
        log("Sunspot data updated from SIDC.")
    else:
        log(f"Failed to fetch sunspot data. HTTP Status Code: {response.status_code}")

# Now load and process the CSV data from the local file.
with open(local_file, "r", newline="") as f:
    csv_data = f.read()

csvfile = StringIO(csv_data)
reader = csv.reader(csvfile, delimiter=";")
header = next(reader)  # Skip header row

for row in reader:
    # Ensure there are at least 8 columns.
    if len(row) < 8:
        continue
    try:
        year = int(row[0])
    except Exception:
        continue
    # Only import data from 2010 onward.
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
    cursor.execute("""
        INSERT OR REPLACE INTO sunspots 
        (year, month, day, fraction, daily_total, std_dev, num_obs, definitive, date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (year, month, day, fraction, daily_total, std_dev, num_obs, definitive, date_str))
conn.commit()

log("Sunspot CSV processed from local file.")


# === Function: Recalculate Cumulative, Hourly, and Daily GDD ===
def recalcGDD(full=False):
    """
    Recalculates cumulative GDD values.

    Modes:
      - Incremental (full=False, default): For each year, only update rows after the last calculated value.
      - Full (full=True): For each year, start at the beginning and recalculate GDD for all rows.
    """
    if full:
        log("Performing full GDD recalculation from the beginning...")
    else:
        log("Performing incremental GDD recalculation...")

    # Get all distinct years present in the readings table.
    cursor.execute("SELECT DISTINCT substr(date, 1, 4) as year FROM readings ORDER BY year ASC")
    years = [row[0] for row in cursor.fetchall()]

    for year in years:
        if full:
            cumulative_gdd = 0
            log(f"For year {year}, starting full recalculation from beginning with cumulative GDD {cumulative_gdd:.3f}.")
            # Process every row for the year from the start.
            cursor.execute(
                "SELECT dateutc, tempf, date FROM readings WHERE substr(date, 1, 4)=? ORDER BY dateutc ASC",
                (year,)
            )
        else:
            # Try to get the last calculated cumulative GDD for this year.
            cursor.execute(
                "SELECT MAX(dateutc), gdd FROM readings WHERE substr(date, 1, 4)=? AND gdd>0",
                (year,)
            )
            result = cursor.fetchone()
            if result[0] is not None:
                last_dateutc = result[0]
                cumulative_gdd = result[1]
                log(f"For year {year}, starting incremental recalculation from dateutc {last_dateutc} with cumulative GDD {cumulative_gdd:.3f}.")
                # Process only rows after the last recalculated timestamp.
                cursor.execute(
                    "SELECT dateutc, tempf, date FROM readings WHERE substr(date, 1, 4)=? AND dateutc > ? ORDER BY dateutc ASC",
                    (year, last_dateutc)
                )
            else:
                cumulative_gdd = 0
                log(f"For year {year}, no previous GDD found. Recalculating from start.")
                cursor.execute(
                    "SELECT dateutc, tempf, date FROM readings WHERE substr(date, 1, 4)=? ORDER BY dateutc ASC",
                    (year,)
                )

        rows = cursor.fetchall()
        for dateutc, tempf, date_str in rows:
            if tempf is None:
                continue
            try:
                val = float(tempf)
            except Exception as e:
                log(f"Skipping record {dateutc} due to invalid tempf: {tempf}")
                continue
            # Convert temperature from Fahrenheit to Celsius.
            temp_c = (val - 32) * 5 / 9
            # Calculate the increment (based on a 5-minute period: 288 intervals per day)
            inc = max(0, (temp_c - base_temp_C)) / 288
            cumulative_gdd += inc
            cursor.execute("UPDATE readings SET gdd = ? WHERE dateutc = ?", (cumulative_gdd, dateutc))
    conn.commit()
    if full:
        log("Full GDD recalculation complete.")
    else:
        log("Incremental GDD recalculation complete.")


# === Fetch Data from Ambient Weather API ===
def fetch_day_data(mac_address, end_date):
    """
    Fetch data for a given day using Ambient Weather’s API.
    Sleeps for API_CALL_DELAY seconds before each call.

    If a 404 (Not Found) is returned, or a 401/403 is returned, the function returns None.
    For 503 errors with a Retry-After > 30 sec, returns None.
    Otherwise, returns the JSON response.
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

        # Handle 404 Not Found
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
def fetch_openmeteo_data(day_str):
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

    # Set the start_date and end_date for the day
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
def fill_missing_data_by_gap(day_str):
    """
    For a given day (YYYY-MM-DD), examine consecutive readings in the database.
    If the gap between two rows exceeds 300 seconds, linearly interpolate missing intervals.
    Interpolated tempf values are rounded to one decimal.
    Inserted rows get is_generated = 1 and mac_source = "INTERP".
    Additionally, fill gaps from the beginning of the day (00:00) to the first reading
    and from the last reading to the expected last reading time (23:55).
    """
    # Calculate expected start and end timestamps for the day.
    dt_day = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    expected_start = int(dt_day.timestamp())
    # Expected last reading is at 23:55 (287 intervals of 300 sec after 00:00)
    expected_end = expected_start + (287 * 300)

    # Get all existing rows for the day.
    cursor.execute(
        "SELECT dateutc, tempf FROM readings WHERE substr(date, 1, 10)=? ORDER BY dateutc ASC",
        (day_str,)
    )
    rows = cursor.fetchall()
    if not rows:
        log(f"No readings found for {day_str} to interpolate.")
        return

    # If the first reading is after expected_start, fill the gap at the beginning.
    first_ts, first_temp = rows[0]
    if first_ts > expected_start:
        gap = first_ts - expected_start
        missing_intervals = gap // 300  # number of intervals missing before first reading
        for j in range(1, missing_intervals + 1):
            new_ts = expected_start + j * 300
            # Use first_temp for extrapolation.
            interp_temp = first_temp
            dt_new = datetime.fromtimestamp(new_ts, tz=timezone.utc)
            new_date_str = dt_new.isoformat() + "Z"
            sql = """
                INSERT OR REPLACE INTO readings
                (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                VALUES (?, ?, ?, 0, 0, 0, 1, "INTERP")
            """
            cursor.execute(sql, (new_ts, new_date_str, round(interp_temp, 1)))
            log(f"Interpolated (start) reading for {new_date_str}: tempf {round(interp_temp, 1)}")

    # Interpolate between existing rows.
    for i in range(len(rows) - 1):
        t1, temp1 = rows[i]
        t2, temp2 = rows[i + 1]
        if temp1 is None or temp2 is None:
            continue
        gap = t2 - t1
        if gap > 300:
            missing_intervals = gap // 300 - 1
            for j in range(1, missing_intervals + 1):
                new_ts = t1 + j * 300
                fraction = j / (missing_intervals + 1)
                interp_temp = temp1 + fraction * (temp2 - temp1)
                interp_temp = round(interp_temp, 1)
                dt_new = datetime.fromtimestamp(new_ts, tz=timezone.utc)
                new_date_str = dt_new.isoformat() + "Z"
                sql = """
                    INSERT OR REPLACE INTO readings
                    (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                    VALUES (?, ?, ?, 0, 0, 0, 1, "INTERP")
                """
                cursor.execute(sql, (new_ts, new_date_str, interp_temp))
                log(f"Interpolated reading for {new_date_str}: tempf {interp_temp:.1f}")

    # If the last reading is before expected_end, fill the gap at the end.
    last_ts, last_temp = rows[-1]
    if last_ts < expected_end:
        gap = expected_end - last_ts
        missing_intervals = gap // 300  # number of intervals missing after last reading
        for j in range(1, missing_intervals + 1):
            new_ts = last_ts + j * 300
            # Use last_temp for extrapolation.
            interp_temp = last_temp
            dt_new = datetime.fromtimestamp(new_ts, tz=timezone.utc)
            new_date_str = dt_new.isoformat() + "Z"
            sql = """
                INSERT OR REPLACE INTO readings
                (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                VALUES (?, ?, ?, 0, 0, 0, 1, "INTERP")
            """
            cursor.execute(sql, (new_ts, new_date_str, round(interp_temp, 1)))
            log(f"Interpolated (end) reading for {new_date_str}: tempf {round(interp_temp, 1)}")
    conn.commit()


##############################
# FORECAST INTEGRATION SECTION
##############################
# New function to append forecast data to the readings table.
def append_forecast_data():
    # Delete all rows with date (YYYY-MM-DD) >= today's date.
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM readings WHERE substr(date, 1, 10) >= ?", (today_str,))
    conn.commit()
    log(f"Deleted existing forecast data from readings table (rows with date >= {today_str}).")

    forecast_df = fetch_openmeteo_forecast()
    if forecast_df is not None and not forecast_df.empty:
        import pandas as pd
        for idx, row in forecast_df.iterrows():
            dt_forecast = row["date"]
            ts = int(dt_forecast.timestamp())
            forecast_date_str = dt_forecast.isoformat() + "Z"
            tempf = row["temperature_2m"]
            sql = """
                INSERT OR REPLACE INTO readings
                (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                VALUES (?, ?, ?, 0, 0, 0, 1, ?)
            """
            try:
                cursor.execute(sql, (ts, forecast_date_str, tempf, "OPENMETEO"))
            except Exception as ex:
                log(f"Error inserting forecast reading for {forecast_date_str}: {ex}")
        conn.commit()
        log(f"Inserted forecast data for {len(forecast_df)} hours into readings.")
    else:
        log("Forecast data unavailable from Open-Meteo.")


##############################
# Main Historical Data Ingestion Loop
##############################
lastRecalcRowCount = 0
new_total = 0
day = START_DATE.date()

while day < CURRENT_DATE:
    # Reload config at the beginning of each day to pick up any changes.
    reload_config()
    # (Optionally, CURRENT_DATE can also be updated here.)

    day_str = day.strftime("%Y-%m-%d")
    cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=?", (day_str,))
    old_count = cursor.fetchone()[0]

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
            cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=?", (day_str,))
            new_count = cursor.fetchone()[0]
            inserted_count = new_count - old_count
            new_total += inserted_count
            log(f"Inserted {inserted_count} new primary readings for {day_str} (total now: {new_count}).")

    # --- Backup Data Insertion (if valid_count is less than 287) ---
    cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=? AND tempf IS NOT NULL", (day_str,))
    valid_count = cursor.fetchone()[0]
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
                cursor.execute("SELECT tempf FROM readings WHERE dateutc = ?", (ts,))
                existing = cursor.fetchone()
                if existing is None:
                    sql = f"""
                        INSERT OR IGNORE INTO readings
                        ({', '.join(fields_order)}, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                        VALUES ({', '.join(['?'] * len(fields_order))}, ?, ?, ?, ?, ?)
                    """
                    try:
                        insert_tuple = tuple(backup_values.get(k, None) for k in fields_order) + (
                        0, 0, 0, 0, BACKUP_MAC_ADDRESS)
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
            cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=? AND tempf IS NOT NULL", (day_str,))
            valid_count = cursor.fetchone()[0]
        else:
            log(f"No backup data received for {day_str}.")

    # --- Open-Meteo Fallback (if still fewer than 287 valid readings) ---
    if valid_count < 287:
        log(f"{day_str}: Only {valid_count} valid readings after backup. Attempting to use Open-Meteo data.")
        df_obj = fetch_openmeteo_data(day_str)
        if df_obj is not None:
            import pandas as pd

            df = df_obj  # fetch_openmeteo_data returns a pandas DataFrame
            if not df.empty:
                for idx, row in df.iterrows():

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
                    tempf = round(row['tempf'], 1)
                    sql = """
                        INSERT OR IGNORE INTO readings
                        (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """
                    try:
                        cursor.execute(sql, (ts, date_str, tempf, 0, 0, 0, 1, "OPENMETEO"))
                    except Exception as ex:
                        log(f"Error inserting Open-Meteo reading for {date_str}: {ex}")
                conn.commit()
                cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=? AND tempf IS NOT NULL",
                               (day_str,))
                valid_count = cursor.fetchone()[0]
                log(f"After Open-Meteo fallback, valid readings for {day_str}: {valid_count}")
            else:
                log(f"No Open-Meteo data received for {day_str}.")
        else:
            log(f"No Open-Meteo data received for {day_str}.")

    # --- Interpolation as Last Resort ---
    if valid_count < 287:
        log(f"{day_str}: Only {valid_count} valid readings after all fallbacks. Filling gaps via interpolation.")
        fill_missing_data_by_gap(day_str)
    else:
        log(f"{day_str}: All intervals have valid temperature data.")

    # --- After interpolation, check the final number of rows for the day ---
    cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date, 1, 10)=?", (day_str,))
    final_day_count = cursor.fetchone()[0]
    log(f"After interpolation, {day_str} has {final_day_count} readings.")

    # --- Periodic Recalculation of GDD ---
    cursor.execute("SELECT COUNT(*) FROM readings")
    totalRowCount = cursor.fetchone()[0]
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
cursor.execute("UPDATE readings SET gdd = NULL")
conn.commit()

log("Performing final full recalculation of cumulative, hourly, and daily GDD...")
recalcGDD(full=True)
conn.commit()
conn.close()
log("Data retrieval complete. Added " + str(new_total) + " new primary readings in total.")
