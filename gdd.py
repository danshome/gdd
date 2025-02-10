import configparser
import os
import sqlite3
import time
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


# === Load Configuration from External File ===
config = configparser.ConfigParser()
config.read('config.ini')

# Global configuration
DB_FILENAME = config.get('global', 'db_filename')
RETRY_SLEEP_TIME = config.getfloat('global', 'retry_sleep_time')
RATE_LIMIT_DELAY = config.getfloat('global', 'rate_limit_delay')
API_CALL_DELAY = config.getfloat('global', 'api_call_delay', fallback=1.0)
DEBUG = config.getboolean('global', 'debug')

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


# === Fetch Data from Ambient Weather API ===
def fetch_day_data(mac_address, end_date):
    """
    Fetch data for a given day using Ambient Weather’s API.
    Sleeps for API_CALL_DELAY seconds before each call.
    """
    url = URL_TEMPLATE.format(
        mac_address=mac_address,
        api_key=API_KEY,
        application_key=APPLICATION_KEY,
        end_date=end_date
    )
    log(f"Calling API URL: {url}")
    while True:
        log_debug(f"Sleeping for RATE_LIMIT_DELAY of {RATE_LIMIT_DELAY} seconds before API call.")
        time.sleep(RATE_LIMIT_DELAY)
        try:
            response = requests.get(url, timeout=10)
        except Exception as ex:
            log(f"Request error for URL {url}: {ex}. Retrying in {RETRY_SLEEP_TIME} seconds...")
            time.sleep(RETRY_SLEEP_TIME)
            continue

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
        # Create a pandas date range from the API’s timestamps
        import pandas as pd
        time_range = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )
        # Get hourly temperature values
        temp_values = hourly.Variables(0).ValuesAsNumpy()
        df = pd.DataFrame({"date": time_range, "tempf": temp_values})
        return df
    else:
        log("No Open-Meteo data received.")
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
    """
    cursor.execute(
        "SELECT dateutc, tempf FROM readings WHERE substr(date, 1, 10)=? ORDER BY dateutc ASC",
        (day_str,)
    )
    rows = cursor.fetchall()
    if not rows:
        log(f"No readings found for {day_str} to interpolate.")
        return
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
                dt = datetime.fromtimestamp(new_ts, tz=timezone.utc)
                new_date_str = dt.isoformat() + "Z"
                sql = """
                    INSERT OR REPLACE INTO readings
                    (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (new_ts, new_date_str, interp_temp, 0, 0, 0, 1, "INTERP"))
                log(f"Interpolated reading for {new_date_str}: tempf {interp_temp:.1f}")
    conn.commit()


# === Main Loop ===
new_total = 0
day = START_DATE.date()

while day <= CURRENT_DATE:
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
        df = fetch_openmeteo_data(day_str)
        if df is not None and not df.empty:
            # Insert each hourly reading from Open-Meteo into the database.
            # For these records, we fill only the dateutc, date, and tempf fields.
            # The other fields will be left as NULL.
            for _, row in df.iterrows():
                # Get the timestamp (in seconds) and ISO string.
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
            cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=? AND tempf IS NOT NULL", (day_str,))
            valid_count = cursor.fetchone()[0]
            log(f"After Open-Meteo fallback, valid readings for {day_str}: {valid_count}")
        else:
            log(f"No Open-Meteo data received for {day_str}.")

    # --- Interpolation as Last Resort ---
    if valid_count < 287:
        log(f"{day_str}: Only {valid_count} valid readings after all fallbacks. Filling gaps via interpolation.")
        fill_missing_data_by_gap(day_str)
    else:
        log(f"{day_str}: All intervals have valid temperature data.")

    day = day + timedelta(days=1)

# === Final Recalculation of Cumulative, Hourly, and Daily GDD ===
log("Recalculating cumulative, hourly, and daily GDD (resetting on Jan 1)...")
cursor.execute("SELECT dateutc, tempf, date FROM readings ORDER BY dateutc ASC")
rows = cursor.fetchall()
cumulative_gdd = 0
current_year = None
for dateutc, tempf, date_str in rows:
    if tempf is None:
        continue
    try:
        val = float(tempf)
    except Exception as e:
        log(f"Skipping record {dateutc} due to invalid tempf: {tempf}")
        continue
    try:
        year = int(date_str[0:4])
    except Exception as e:
        log(f"Could not parse year from date {date_str} for record {dateutc}")
        continue
    if current_year is None or current_year != year:
        cumulative_gdd = 0
        current_year = year
    temp_c = (val - 32) * 5 / 9
    inc = max(0, (temp_c - base_temp_C)) / 288
    cumulative_gdd += inc
    cursor.execute("UPDATE readings SET gdd = ? WHERE dateutc = ?", (cumulative_gdd, dateutc))

cursor.execute("SELECT DISTINCT substr(date, 1, 13) AS hour_key FROM readings ORDER BY hour_key ASC")
hour_keys = [row[0] for row in cursor.fetchall()]
cumulative_hourly = 0
current_year_hour = None
for hour_key in hour_keys:
    try:
        year = int(hour_key[:4])
    except Exception as e:
        log(f"Could not parse year from hour_key {hour_key}")
        continue
    if current_year_hour is None or current_year_hour != year:
        cumulative_hourly = 0
        current_year_hour = year
    cursor.execute("SELECT MAX(tempf), MIN(tempf) FROM readings WHERE substr(date, 1, 13) = ?", (hour_key,))
    res = cursor.fetchone()
    if res is None or res[0] is None or res[1] is None:
        continue
    hour_max = float(res[0])
    hour_min = float(res[1])
    hour_avg = (hour_max + hour_min) / 2.0
    hourly_period_gdd = max(0, hour_avg - base_temp_F) / 24.0
    cumulative_hourly += hourly_period_gdd
    cursor.execute("UPDATE readings SET gdd_hourly = ? WHERE substr(date, 1, 13) = ?", (cumulative_hourly, hour_key))

cursor.execute("SELECT DISTINCT substr(date, 1, 10) AS day_key FROM readings ORDER BY day_key ASC")
day_keys = [row[0] for row in cursor.fetchall()]
cumulative_daily = 0
current_year_day = None
for day_key in day_keys:
    try:
        year = int(day_key[:4])
    except Exception as e:
        log(f"Could not parse year from day_key {day_key}")
        continue
    if current_year_day is None or current_year_day != year:
        cumulative_daily = 0
        current_year_day = year
    cursor.execute("SELECT MAX(tempf), MIN(tempf) FROM readings WHERE substr(date, 1, 10) = ?", (day_key,))
    res = cursor.fetchone()
    if res is None or res[0] is None or res[1] is None:
        continue
    day_max = float(res[0])
    day_min = float(res[1])
    day_avg = (day_max + day_min) / 2.0
    daily_period_gdd = max(0, day_avg - base_temp_F)
    cumulative_daily += daily_period_gdd
    cursor.execute("UPDATE readings SET gdd_daily = ? WHERE substr(date, 1, 10) = ?", (cumulative_daily, day_key))

conn.commit()
conn.close()
log(f"Data retrieval complete. Added {new_total} new primary readings in total.")
