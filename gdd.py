#!/usr/bin/env python3
"""
GDD Calculation and Data Ingestion Script

This module performs the following:
  - Reads configuration from config.ini.
  - Establishes a connection to a SQLite database.
  - Creates required tables (readings, grapevine_gdd, vineyard_pests, sunspots).
  - Imports data from CSV files (grapevine, vineyard pests, sunspots).
  - Fetches weather data from Ambient Weather API and Open-Meteo as fallback.
  - Recalculates cumulative Growing Degree Days (GDD) both incrementally and fully.
  - Fills in missing readings via linear interpolation.
  - Appends forecast data.
  - Projects bud break dates for grape varieties using historical regression.

All functions are documented, and complex sections include inline comments for clarity.
"""

import configparser
import sqlite3
import sys
import time
import requests
import subprocess
import csv
from io import StringIO
from datetime import datetime, timedelta, timezone
import pandas as pd
import xgboost as xgb
import numpy as np
from sklearn.model_selection import cross_val_score
import pickle
import os


# --- Constants ---
CONFIG_FILE = "config.ini"
GRAPEVINE_CSV = "grapevine_gdd.csv"
SUNSPOT_CSV = "SN_d_tot_V2.0.csv"
VINEYARD_PESTS_CSV = "vineyard_pests.csv"

# Global variables (populated by reload_config)
DB_FILENAME = None
RETRY_SLEEP_TIME = None
RATE_LIMIT_DELAY = None
API_CALL_DELAY = None
DEBUG = False
RECALC_INTERVAL = None
MAC_ADDRESS = None
API_KEY = None
APPLICATION_KEY = None
BACKUP_MAC_ADDRESS = None
URL_TEMPLATE = None
START_DATE = None
CURRENT_DATE = None
OPENMETEO_LAT = None
OPENMETEO_LON = None
BUD_BREAK_START = None
FORECAST_DAYS = None
FORECAST_MODEL = None
HISTORICAL_WINDOW_DAYS = None
MAX_GAP_SECONDS = 6 * 3600  # 6 hours: gaps larger than this trigger Open-Meteo historical fetch

# Base temperatures for GDD calculations
BASE_TEMP_C = 10  # °C threshold for 5-minute cumulative GDD
BASE_TEMP_F = 50  # °F threshold for hourly/daily calculations

# Order of fields when inserting into the readings table
FIELDS_ORDER = [
    "dateutc", "date", "tempf", "humidity", "baromrelin", "baromabsin", "feelsLike",
    "dewPoint", "winddir", "windspeedmph", "windgustmph", "maxdailygust", "windgustdir",
    "winddir_avg2m", "windspdmph_avg2m", "winddir_avg10m", "windspdmph_avg10m",
    "hourlyrainin", "dailyrainin", "monthlyrainin", "yearlyrainin", "battin", "battout",
    "tempinf", "humidityin", "feelsLikein", "dewPointin", "lastRain", "passkey", "time", "loc"
]


# --- Logging Functions ---
def log(message: str) -> None:
    """
    Logs a message with the current UTC timestamp in the format
    YYYY-MM-DD HH:MM:SS.mmm (milliseconds precision).

    :param message: The message to log.
    :type message: str
    :return: None
    """
    print(f"[{datetime.now().isoformat()}] {message}")


def log_debug(message: str) -> None:
    """
    Logs a debug message if the debug mode is enabled.

    This function appends a "[DEBUG]" prefix to the input message and logs it
    only when the debug mode is active, as indicated by the `DEBUG` global
    variable.
    """
    if DEBUG:
        log("[DEBUG] " + message)


# --- Configuration Reload ---
def reload_config() -> None:
    """
    Reloads the configuration settings from a specified configuration file.
    """
    global DB_FILENAME, RETRY_SLEEP_TIME, RATE_LIMIT_DELAY, API_CALL_DELAY, DEBUG, RECALC_INTERVAL, \
        MAC_ADDRESS, API_KEY, APPLICATION_KEY, BACKUP_MAC_ADDRESS, URL_TEMPLATE, START_DATE, CURRENT_DATE, \
        OPENMETEO_LAT, OPENMETEO_LON, BUD_BREAK_START, GRAPEVINE_CSV, SUNSPOT_CSV, \
        FORECAST_DAYS, FORECAST_MODEL, HISTORICAL_WINDOW_DAYS

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

    # API endpoint configuration
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

    try:
        BUD_BREAK_START = datetime.strptime(config.get('date', 'bud_break_start'), "%Y-%m-%d").date()
    except Exception:
        BUD_BREAK_START = datetime(datetime.now(timezone.utc).year, 1, 1).date()

    # Open-Meteo configuration (fallback)
    try:
        OPENMETEO_LAT = config.getfloat('openmeteo', 'latitude')
        OPENMETEO_LON = config.getfloat('openmeteo', 'longitude')
        FORECAST_DAYS = config.getint('openmeteo', 'forecast_days', fallback=16)
        FORECAST_MODEL = config.get('openmeteo', 'forecast_model', fallback='best_match')
        HISTORICAL_WINDOW_DAYS = config.getint('global', 'historical_window_days', fallback=16)
    except Exception as e:
        log(f"Error in Open-Meteo configuration: {e}")
        sys.exit(1)

    # Optional CSV file overrides
    GRAPEVINE_CSV = config.get('files', 'grapevine_csv', fallback=GRAPEVINE_CSV)
    SUNSPOT_CSV = config.get('files', 'sunspot_csv', fallback=SUNSPOT_CSV)


# --- Database Functions ---
def ensure_database_exists() -> None:
    """
    Ensures that the SQLite database file exists.
    """
    if not os.path.exists(DB_FILENAME):
        log("Database file not found. Attempting to retrieve it using 'dvc pull'...")
        try:
            subprocess.check_call(["dvc", "pull", DB_FILENAME])
        except subprocess.CalledProcessError as e:
            log(f"Error during dvc pull: {e}")
        if not os.path.exists(DB_FILENAME):
            answer = input("Database file still missing after dvc pull. Create a new database? (y/n): ")
            if answer.lower().startswith("y"):
                log("Proceeding with creation of a new database.")
            else:
                log("Exiting program.")
                sys.exit(1)


def get_db_connection() -> sqlite3.Connection:
    """
    Establishes a connection to the SQLite database and returns the connection object.
    """
    try:
        conn = sqlite3.connect(DB_FILENAME)
        return conn
    except Exception as e:
        log(f"Error connecting to database: {e}")
        sys.exit(1)


def execute_sql(cursor: sqlite3.Cursor, statement: str, params=()) -> None:
    """
    Executes a provided SQL statement with optional parameters using the given SQLite cursor.
    """
    try:
        cursor.execute(statement, params)
    except sqlite3.Error as e:
        log(f"SQL error: {e} while executing: {statement} with params: {params}")
        cursor.connection.rollback()


# --- Table Creation Functions ---
def create_tables(conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> None:
    """
    Creates and initializes the required database tables and indexes.
    """
    # Create the readings table
    execute_sql(cursor, """
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
    execute_sql(cursor, "CREATE INDEX IF NOT EXISTS idx_readings_day ON readings (substr(date, 1, 10));")
    execute_sql(cursor, "CREATE INDEX IF NOT EXISTS idx_gdd ON readings (gdd);")
    execute_sql(cursor, "CREATE INDEX IF NOT EXISTS idx_readings_year ON readings((substr(date, 1, 4)));")
    execute_sql(cursor,
                "CREATE INDEX IF NOT EXISTS idx_readings_month ON readings((cast(substr(date, 6, 2) as integer)));")
    execute_sql(cursor, "CREATE INDEX IF NOT EXISTS idx_readings_date ON readings (date);")
    # execute_sql(cursor,
    #             "CREATE INDEX IF NOT EXISTS idx_readings_year_date_gdd ON readings (substr(date, 1, 4), date, gdd);")
    conn.commit()

    # Create the grapevine_gdd table with new columns for biofix and accumulated GDD
    execute_sql(cursor, """
    CREATE TABLE IF NOT EXISTS grapevine_gdd (
        variety TEXT PRIMARY KEY,
        heat_summation INTEGER,
        biofix_date TEXT DEFAULT (date('now','start of year')),
        gdd REAL DEFAULT 0
    );
    """)
    conn.commit()

    # Create the vineyard_pests table
    execute_sql(cursor, """
    CREATE TABLE IF NOT EXISTS vineyard_pests (
        sequence_id INTEGER PRIMARY KEY,
        common_name TEXT,
        scientific_name TEXT,
        dormant INTEGER CHECK (dormant IN (0,1)),
        stage TEXT,
        min_gdd INTEGER,
        max_gdd INTEGER
    );
    """)
    conn.commit()
    # execute_sql(cursor, "CREATE INDEX IF NOT EXISTS idx_vineyard_pests_gdd ON vineyard_pests(min_gdd, max_gdd);")
    conn.commit()

    # Create the sunspots table
    execute_sql(cursor, """
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
    # execute_sql(cursor, "CREATE INDEX IF NOT EXISTS idx_sunspots_date ON sunspots(date);")
    execute_sql(cursor, "CREATE INDEX IF NOT EXISTS idx_sunspots_year ON sunspots((substr(date, 1, 4)));")
    execute_sql(cursor,
                "CREATE INDEX IF NOT EXISTS idx_sunspots_month ON sunspots((cast(substr(date, 6, 2) as integer)));")
    conn.commit()


# --- Data Import Functions ---
def import_grapevine_csv(cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Imports data from the Grapevine CSV file into the grapevine_gdd table.
    Uses an UPSERT so that an existing biofix_date is preserved.
    """
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
                # Use UPSERT to update heat_summation without overwriting biofix_date.
                execute_sql(cursor, """
                    INSERT INTO grapevine_gdd (variety, heat_summation)
                    VALUES (?, ?)
                    ON CONFLICT(variety) DO UPDATE SET heat_summation = excluded.heat_summation
                """, (variety, heat_summation))
        conn.commit()
    except Exception as e:
        log(f"Error processing {GRAPEVINE_CSV}: {e}")


def import_vineyard_pests(cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Imports vineyard pest data from a CSV file into the vineyard_pests table.
    """
    try:
        df_pests = pd.read_csv(VINEYARD_PESTS_CSV)
    except Exception as e:
        log(f"Error reading {VINEYARD_PESTS_CSV}: {e}")
        return

    for index, row in df_pests.iterrows():
        try:
            execute_sql(cursor, """
                INSERT OR REPLACE INTO vineyard_pests
                (sequence_id, common_name, scientific_name, dormant, stage, min_gdd, max_gdd)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                row['sequence_id'],
                row['common_name'],
                row['scientific_name'],
                row['dormant'],
                row['stage'],
                row['gdd_min'],
                row['gdd_max']
            ))
        except Exception as e:
            log(f"Error inserting pest data for {row['common_name']} (Row: {index}): {e}")
    conn.commit()
    log("Vineyard pests table updated from CSV.")


def import_sunspots_data(cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Imports sunspot data from the SIDC CSV file into the sunspots table.
    """
    sunspot_url = "https://www.sidc.be/SILSO/INFO/sndtotcsv.php?"
    download_file = True
    try:
        head_response = requests.head(sunspot_url)
        remote_last_modified = None
        if head_response.status_code == 200:
            remote_last_modified_str = head_response.headers.get("Last-Modified")
            if remote_last_modified_str:
                try:
                    remote_last_modified = datetime.strptime(remote_last_modified_str,
                                                             "%a, %d %b %Y %H:%M:%S GMT").replace(tzinfo=timezone.utc)
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
            execute_sql(cursor, """
                INSERT OR REPLACE INTO sunspots 
                (year, month, day, fraction, daily_total, std_dev, num_obs, definitive, date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (year, month, day, fraction, daily_total, std_dev, num_obs, definitive, date_str))
        conn.commit()
        log("Sunspot CSV processed from local file.")
    except Exception as e:
        log(f"Error processing {SUNSPOT_CSV}: {e}")


# --- New Function: Recalculate Varietal GDD Using biofix_date ---
def recalc_varietal_gdd(cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    For each grapevine variety, calculates the cumulative GDD from its biofix_date.
    Uses the formula: inc = max(0, (temp_c - BASE_TEMP_C)) / 288, summing over readings where date >= biofix_date.
    Updates the grapevine_gdd.gdd column with the accumulated value.
    """
    cursor.execute("SELECT variety, COALESCE(biofix_date, date('now','start of year')) FROM grapevine_gdd")
    rows = cursor.fetchall()
    for variety, biofix_date in rows:
        # Ensure we have a complete ISO datetime string (assume midnight UTC if only date provided)
        start_iso = biofix_date if "T" in biofix_date else biofix_date + "T00:00:00Z"
        cursor.execute("SELECT tempf FROM readings WHERE date >= ? ORDER BY date ASC", (start_iso,))
        readings = cursor.fetchall()
        cumulative = 0.0
        for (tempf,) in readings:
            if tempf is None:
                continue
            try:
                temp = float(tempf)
            except Exception:
                continue
            temp_c = (temp - 32) * 5 / 9
            inc = max(0, temp_c - BASE_TEMP_C) / 288
            cumulative += inc
        execute_sql(cursor, "UPDATE grapevine_gdd SET gdd = ? WHERE variety = ?", (cumulative, variety))
        log(f"Updated {variety}: biofix_date={biofix_date}, accumulated GDD={cumulative:.3f}")
    conn.commit()


# --- Data Processing Functions (Existing) ---
def recalc_gdd(cursor: sqlite3.Cursor, conn: sqlite3.Connection, full: bool = False) -> None:
    """
    Recalculate Growing Degree Days (GDD) for weather readings.
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
                cursor.execute(
                    "SELECT dateutc, tempf, date FROM readings WHERE substr(date, 1, 4)=? ORDER BY dateutc ASC",
                    (year,))
            else:
                cursor.execute("SELECT MAX(dateutc), gdd FROM readings WHERE substr(date, 1, 4)=? AND gdd>0", (year,))
                result = cursor.fetchone()
                if result[0] is not None:
                    last_dateutc = result[0]
                    cumulative_gdd = result[1]
                    log(f"For year {year}, starting incremental recalculation from dateutc {last_dateutc} with cumulative GDD {cumulative_gdd:.3f}.")
                    cursor.execute(
                        "SELECT dateutc, tempf, date FROM readings WHERE substr(date, 1, 4)=? AND dateutc > ? ORDER BY dateutc ASC",
                        (year, last_dateutc))
                else:
                    cumulative_gdd = 0
                    log(f"For year {year}, no previous GDD found. Recalculating from start.")
                    cursor.execute(
                        "SELECT dateutc, tempf, date FROM readings WHERE substr(date, 1, 4)=? ORDER BY dateutc ASC",
                        (year,))
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
                inc = max(0, (temp_c - BASE_TEMP_C)) / 288
                cumulative_gdd += inc
                execute_sql(cursor, "UPDATE readings SET gdd = ? WHERE dateutc = ?", (cumulative_gdd, dateutc))
        except sqlite3.Error as e:
            log(f"Error during GDD recalculation for year {year}: {e}")
    conn.commit()
    if full:
        log("Full GDD recalculation complete.")
    else:
        log("Incremental GDD recalculation complete.")


def calculate_chill_hours(cursor: sqlite3.Cursor, start_date: datetime, end_date: datetime, threshold: float = 45) -> float:
    cursor.execute(
        "SELECT COUNT(*) FROM readings WHERE date >= ? AND date <= ? AND tempf < ?",
        (start_date.isoformat(), end_date.isoformat(), threshold)
    )
    chill_intervals = cursor.fetchone()[0]
    chill_hours = chill_intervals * (5 / 60)
    return chill_hours


def calculate_historical_gdds(cursor: sqlite3.Cursor, years: list, doy: int) -> list:
    historical_gdds = []
    for year in years:
        target_date = datetime(year, 1, 1) + timedelta(days=doy - 1)
        cursor.execute(
            "SELECT MAX(gdd) FROM readings WHERE substr(date, 1, 4)=? AND date <= ?",
            (str(year), target_date.isoformat())
        )
        gdd = cursor.fetchone()[0]
        historical_gdds.append(gdd if gdd is not None else 0)
    return historical_gdds


def calculate_avg_daily_gdd(cursor: sqlite3.Cursor, years: list, current_date: datetime) -> float:
    rates = []
    for year in years:
        start_date = datetime(year, current_date.month, current_date.day)
        end_date = start_date + timedelta(days=HISTORICAL_WINDOW_DAYS)
        cursor.execute(
            "SELECT MAX(gdd), MIN(gdd) FROM readings WHERE substr(date, 1, 4)=? AND date BETWEEN ? AND ?",
            (str(year), start_date.isoformat(), end_date.isoformat())
        )
        max_gdd, min_gdd = cursor.fetchone()
        if max_gdd and min_gdd:
            rate = (max_gdd - min_gdd) / 14
            rates.append(rate)
    return sum(rates) / len(rates) if rates else 2.0


def fetch_day_data(mac_address: str, end_date: str) -> any:
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


def fetch_openmeteo_data(day_str: str) -> any:
    try:
        import openmeteo_requests
        import requests_cache
        from retry_requests import retry
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


def insert_openmeteo_historical(cursor: sqlite3.Cursor, conn: sqlite3.Connection, day_str: str) -> int:
    """
    Fetches hourly historical data from Open-Meteo for a given day and inserts
    readings into the database. Uses INSERT OR IGNORE so real station data is
    never overwritten.

    Returns the number of rows inserted.
    """
    df = fetch_openmeteo_data(day_str)
    if df is None or df.empty:
        log(f"No Open-Meteo historical data available for {day_str}.")
        return 0
    inserted = 0
    for _, row in df.iterrows():
        dt_om = row["date"]
        ts = int(dt_om.timestamp())
        date_str = dt_om.isoformat() + "Z"
        tempf = row["tempf"]
        if pd.isna(tempf):
            continue
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO readings
                (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                VALUES (?, ?, ?, 0, 0, 0, 1, ?)
            """, (ts, date_str, float(tempf), "OPENMETEO"))
            if cursor.rowcount > 0:
                inserted += 1
        except Exception as ex:
            log(f"Error inserting Open-Meteo historical reading for {date_str}: {ex}")
    conn.commit()
    if inserted > 0:
        log(f"Open-Meteo historical: inserted {inserted} hourly readings for {day_str}.")
    return inserted


def fetch_openmeteo_forecast() -> any:
    try:
        import openmeteo_requests
        import requests_cache
        from retry_requests import retry
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
        "models": FORECAST_MODEL,
        "forecast_days": FORECAST_DAYS
    }
    log(f"Fetching hourly forecast data from Open-Meteo with params: {params}")
    responses = openmeteo.weather_api(url, params=params)
    if responses:
        response = responses[0]
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
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


def fill_missing_data_by_gap(cursor: sqlite3.Cursor, conn: sqlite3.Connection, day_str: str) -> None:
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

    available = {}
    for ts, temp in rows:
        snapped = expected_start + ((ts - expected_start) // 300) * 300
        if snapped not in available:
            available[snapped] = temp

    # Cross-midnight anchors: fetch boundary readings from adjacent days
    # These participate in interpolation but don't generate new rows themselves
    anchor_timestamps = set()
    prev_row = None
    next_row = None
    try:
        # Last reading before this day's midnight (from previous day)
        cursor.execute(
            "SELECT dateutc, tempf FROM readings WHERE dateutc < ? AND tempf IS NOT NULL ORDER BY dateutc DESC LIMIT 1",
            (expected_start,)
        )
        prev_row = cursor.fetchone()
        if prev_row:
            available[prev_row[0]] = prev_row[1]
            anchor_timestamps.add(prev_row[0])
        # First reading after this day ends (from next day)
        cursor.execute(
            "SELECT dateutc, tempf FROM readings WHERE dateutc > ? AND tempf IS NOT NULL ORDER BY dateutc ASC LIMIT 1",
            (expected_end,)
        )
        next_row = cursor.fetchone()
        if next_row:
            available[next_row[0]] = next_row[1]
            anchor_timestamps.add(next_row[0])
    except sqlite3.Error as e:
        log(f"Error fetching cross-midnight anchors for {day_str}: {e}")

    # Gap-size analysis: if the largest consecutive gap exceeds MAX_GAP_SECONDS,
    # fetch Open-Meteo hourly data to add intermediate anchor points
    sorted_ts = sorted(available.keys())
    if len(sorted_ts) >= 2:
        max_gap = max(sorted_ts[i + 1] - sorted_ts[i] for i in range(len(sorted_ts) - 1))
        if max_gap > MAX_GAP_SECONDS:
            log(f"{day_str}: Largest gap is {max_gap}s (>{MAX_GAP_SECONDS}s). Fetching Open-Meteo historical data.")
            om_count = insert_openmeteo_historical(cursor, conn, day_str)
            if om_count > 0:
                # Rebuild available dict with the new Open-Meteo readings
                cursor.execute(
                    "SELECT dateutc, tempf FROM readings WHERE substr(date, 1, 10)=? ORDER BY dateutc ASC",
                    (day_str,)
                )
                rows = cursor.fetchall()
                available = {}
                for ts, temp in rows:
                    snapped = expected_start + ((ts - expected_start) // 300) * 300
                    if snapped not in available:
                        available[snapped] = temp
                # Re-add cross-midnight anchors
                anchor_timestamps = set()
                if prev_row:
                    available[prev_row[0]] = prev_row[1]
                    anchor_timestamps.add(prev_row[0])
                if next_row:
                    available[next_row[0]] = next_row[1]
                    anchor_timestamps.add(next_row[0])

    grid_points = sorted(available.keys())
    for point in expected_points:
        if point in available:
            continue
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
            continue
        dt_new = datetime.fromtimestamp(point, tz=timezone.utc)
        new_date_str = dt_new.isoformat() + "Z"
        execute_sql(cursor, """
                INSERT OR REPLACE INTO readings
                (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                VALUES (?, ?, ?, 0, 0, 0, 1, "INTERP")
        """, (point, new_date_str, interp_temp))
        log(f"Interpolated reading for {new_date_str}: tempf {interp_temp:.1f}")
    conn.commit()


def append_forecast_data(cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        execute_sql(cursor, "DELETE FROM readings WHERE substr(date, 1, 10) >= ?", (today_str,))
        conn.commit()
    except sqlite3.Error as e:
        log(f"Error deleting forecast data: {e}")
    log(f"Deleted existing forecast data from readings table (rows with date >= {today_str}).")

    forecast_df = fetch_openmeteo_forecast()
    if forecast_df is not None and not forecast_df.empty:
        for idx, row in forecast_df.iterrows():
            dt_forecast = row["date"]
            ts = int(dt_forecast.timestamp())
            forecast_date_str = dt_forecast.isoformat() + "Z"
            tempf = row["temperature_2m"]
            try:
                execute_sql(cursor, """
                    INSERT OR REPLACE INTO readings
                    (dateutc, date, tempf, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                    VALUES (?, ?, ?, 0, 0, 0, 1, ?)
                """, (ts, forecast_date_str, tempf, "OPENMETEO"))
            except Exception as ex:
                log(f"Error inserting forecast reading for {forecast_date_str}: {ex}")
        conn.commit()
        log(f"Inserted forecast data for {len(forecast_df)} hours into readings.")

        forecast_days = set(row["date"].date() for idx, row in forecast_df.iterrows())
        for day in sorted(forecast_days):
            day_str = day.strftime("%Y-%m-%d")
            log(f"Interpolating missing data for forecast day: {day_str}")
            fill_missing_data_by_gap(cursor, conn, day_str)
        conn.commit()
        log("Completed interpolation for all forecast days.")
    else:
        log("Forecast data unavailable from Open-Meteo.")


# --- Updated Bud Break Regression Using biofix_date ---
def project_bud_break_regression(cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    """
    Executes a process to analyze historical data and forecast projected bud-break dates for grapevine varieties.
    This version uses each variety's biofix_date as the start of the growing season.
    """
    column_name = "regression_projected_bud_break"
    try:
        execute_sql(cursor, f"ALTER TABLE grapevine_gdd ADD COLUMN {column_name} TEXT")
        conn.commit()
        log(f"Added column {column_name} to grapevine_gdd.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            log(f"Column {column_name} already exists, skipping addition.")
        else:
            log(f"Error adding column {column_name}: {e}")

    current_year = datetime.now(timezone.utc).year
    try:
        cursor.execute("SELECT MIN(substr(date, 1, 4)) FROM readings")
        oldest_year_str = cursor.fetchone()[0]
        oldest_year = int(oldest_year_str) if oldest_year_str else current_year
    except sqlite3.Error as e:
        log(f"Error fetching oldest year: {e}")
        oldest_year = current_year

    historical_years = list(range(oldest_year, current_year))
    try:
        cursor.execute("SELECT variety, heat_summation, biofix_date FROM grapevine_gdd")
        varieties = cursor.fetchall()
    except sqlite3.Error as e:
        log(f"Error fetching grapevine_gdd data: {e}")
        return

    for variety, heat_sum, biofix_date in varieties:
        if heat_sum is None:
            log(f"Skipping {variety} due to undefined heat_summation.")
            continue
        # Use biofix_date as start; if missing, default to Jan 1 of that year.
        if biofix_date is None:
            biofix_date = f"{current_year}-01-01"
        try:
            bd_dt = datetime.strptime(biofix_date, "%Y-%m-%d")
        except Exception as ex:
            bd_dt = datetime(current_year, 1, 1)
        biofix_md = bd_dt.strftime("%m-%d")
        data_points = []
        for yr in historical_years:
            start_date_str = f"{yr}-{biofix_md}T00:00:00Z"
            try:
                cursor.execute(
                    "SELECT date FROM readings WHERE date >= ? AND gdd >= ? ORDER BY date ASC LIMIT 1",
                    (start_date_str, heat_sum)
                )
                row = cursor.fetchone()
                if row:
                    dt = datetime.fromisoformat(row[0].rstrip("Z"))
                    bud_break_doy = dt.timetuple().tm_yday
                    data_points.append((yr, bud_break_doy))
            except sqlite3.Error as e:
                log(f"Error fetching readings for year {yr}: {e}")
        if len(data_points) < 2:
            log(f"Not enough historical data for {variety} in regression model; skipping.")
            continue

        mean_year = sum(x for x, _ in data_points) / len(data_points)
        mean_doy = sum(y for _, y in data_points) / len(data_points)
        numerator = sum((x - mean_year) * (y - mean_doy) for x, y in data_points)
        denominator = sum((x - mean_year) ** 2 for x, y in data_points)
        slope = numerator / denominator if denominator != 0 else 0
        intercept = mean_doy - slope * mean_year
        predicted_doy = slope * current_year + intercept
        predicted_doy = max(1, min(366, predicted_doy))
        predicted_date = (datetime(current_year, 1, 1) + timedelta(days=predicted_doy - 1)).date()

        execute_sql(cursor, f"""
            UPDATE grapevine_gdd SET {column_name} = ? WHERE variety = ?
        """, (predicted_date.isoformat(), variety))
        log(f"Regression predicted bud break for {variety}: {predicted_date.isoformat()} (slope: {slope:.2f}, intercept: {intercept:.2f})")
    conn.commit()


# --- Existing Bud Break Projection Functions (Hybrid and EHML) ---
def project_bud_break_hybrid(cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    for col in ["hybrid_projected_bud_break", "hybrid_bud_break_range"]:
        try:
            execute_sql(cursor, f"ALTER TABLE grapevine_gdd ADD COLUMN {col} TEXT")
            conn.commit()
            log(f"Added column {col} to grapevine_gdd.")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                log(f"Column {col} already exists, skipping addition.")
            else:
                log(f"Error adding column {col}: {e}")

    current_date = datetime.now(timezone.utc).date()
    current_year = current_date.year

    cursor.execute("SELECT DISTINCT substr(date, 1, 4) AS year FROM readings ORDER BY year ASC")
    years = [int(row[0]) for row in cursor.fetchall()]
    historical_years = [y for y in years if y < current_year]

    cursor.execute("SELECT variety, heat_summation FROM grapevine_gdd")
    varieties = cursor.fetchall()

    for variety, heat_sum in varieties:
        if heat_sum is None:
            log(f"Skipping {variety} due to undefined heat_summation.")
            continue

        bud_break_gdd = []
        bud_break_doys = []
        for yr in historical_years:
            cursor.execute(
                "SELECT date, gdd FROM readings WHERE substr(date, 1, 4)=? AND gdd >= ? ORDER BY date ASC LIMIT 1",
                (str(yr), heat_sum)
            )
            row = cursor.fetchone()
            if row:
                dt = datetime.fromisoformat(row[0].rstrip("Z"))
                bud_break_doys.append(dt.timetuple().tm_yday)
                bud_break_gdd.append(row[1])

        if len(bud_break_gdd) < 1:
            log(f"Not enough historical data for {variety} in hybrid model; skipping.")
            continue

        target_gdd = sorted(bud_break_gdd)[len(bud_break_gdd) // 2]
        doy_std = (sum((d - sum(bud_break_doys) / len(bud_break_doys)) ** 2 for d in bud_break_doys) / len(bud_break_doys)) ** 0.5

        cursor.execute(
            "SELECT MAX(gdd) FROM readings WHERE substr(date, 1, 4)=? AND date <= ?",
            (str(current_year), current_date.isoformat())
        )
        current_gdd = cursor.fetchone()[0] or 0

        forecast_end = (current_date + timedelta(days=FORECAST_DAYS)).isoformat()
        cursor.execute(
            "SELECT MAX(gdd) FROM readings WHERE substr(date, 1, 4)=? AND date <= ?",
            (str(current_year), forecast_end)
        )
        end_gdd = cursor.fetchone()[0] or current_gdd
        forecast_gdd = max(0, end_gdd - current_gdd)
        total_gdd = current_gdd + forecast_gdd

        remaining_gdd = max(0, target_gdd - total_gdd)
        if remaining_gdd == 0:
            cursor.execute(
                "SELECT date FROM readings WHERE substr(date, 1, 4)=? AND gdd >= ? ORDER BY date ASC LIMIT 1",
                (str(current_year), target_gdd)
            )
            row = cursor.fetchone()
            predicted_date = datetime.fromisoformat(row[0].rstrip("Z")).date() if row else current_date + timedelta(days=14)
        else:
            historical_rates = []
            for yr, doy in zip(historical_years, bud_break_doys):
                start_date = datetime(yr, 1, 1) + timedelta(days=(current_date.timetuple().tm_yday - 1))
                cursor.execute(
                    "SELECT gdd FROM readings WHERE substr(date, 1, 4)=? AND date <= ? ORDER BY date DESC LIMIT 1",
                    (str(yr), start_date.isoformat())
                )
                start_gdd = cursor.fetchone()[0] or 0
                days_diff = doy - start_date.timetuple().tm_yday
                rate = (bud_break_gdd[historical_years.index(yr)] - start_gdd) / days_diff if days_diff > 0 else 2.0
                if rate > 0:
                    historical_rates.append(rate)
            avg_daily_gdd = sum(historical_rates) / len(historical_rates) if historical_rates else 2.0
            days_remaining = min(remaining_gdd / avg_daily_gdd, 90)
            predicted_date = current_date + timedelta(days=14 + days_remaining)

        log(f"{variety}: heat_sum={heat_sum}, target_gdd={target_gdd}, current_gdd={current_gdd}, "
            f"forecast_gdd={forecast_gdd}, remaining_gdd={remaining_gdd}, avg_daily_gdd={avg_daily_gdd}, "
            f"days_remaining={days_remaining}")

        predicted_doy = predicted_date.timetuple().tm_yday
        range_start = max(1, predicted_doy - doy_std)
        range_end = min(366, predicted_doy + doy_std)
        range_start_date = (datetime(current_year, 1, 1) + timedelta(days=range_start - 1)).isoformat()
        range_end_date = (datetime(current_year, 1, 1) + timedelta(days=range_end - 1)).isoformat()
        range_str = f"{range_start_date},{range_end_date}"

        execute_sql(cursor, """
            UPDATE grapevine_gdd SET hybrid_projected_bud_break = ?, hybrid_bud_break_range = ? WHERE variety = ?
        """, (predicted_date.isoformat(), range_str, variety))
        log(f"Hybrid predicted bud break for {variety}: {predicted_date.isoformat()} (±{doy_std:.1f} days)")
    conn.commit()


def project_bud_break_ehml(cursor: sqlite3.Cursor, conn: sqlite3.Connection) -> None:
    log("Starting EHML bud break projection.")

    # Define prediction column
    column_name = "ehml_projected_bud_break"
    try:
        cursor.execute(f"ALTER TABLE grapevine_gdd ADD COLUMN {column_name} TEXT")
        conn.commit()
        log(f"Added column {column_name} to grapevine_gdd.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            log(f"Column {column_name} already exists, skipping.")
        else:
            log(f"Error adding column: {e}")

    # Create training data table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ehml_training_data (
            variety TEXT,
            year INTEGER,
            current_gdd REAL,
            doy INTEGER,
            chill_hours REAL,
            mean_gdd REAL,
            std_gdd REAL,
            remaining_gdd REAL,
            PRIMARY KEY (variety, year)
        )
    """)
    conn.commit()

    # Current date info
    current_date = datetime.now().date()
    current_year = current_date.year
    doy = current_date.timetuple().tm_yday
    log(f"Current date: {current_date}, DOY: {doy}, Year: {current_year}")

    # Fetch historical years
    cursor.execute("""
        SELECT DISTINCT CAST(substr(date, 1, 4) AS INTEGER) AS year 
        FROM readings 
        WHERE CAST(substr(date, 1, 4) AS INTEGER) < ? 
        ORDER BY year ASC
    """, (current_year,))
    historical_years = [row[0] for row in cursor.fetchall()]
    log(f"Found {len(historical_years)} historical years.")

    if not historical_years:
        log("Error: No historical years found.")
        return

    # Fetch varieties
    cursor.execute("SELECT variety, heat_summation FROM grapevine_gdd")
    varieties_data = cursor.fetchall()
    log(f"Found {len(varieties_data)} varieties.")

    # Helper functions
    def calculate_daily_chill_hours(cursor, date):
        date_str = date.strftime('%Y-%m-%d')
        next_day_str = (date + timedelta(days=1)).strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT COUNT(*) FROM readings 
            WHERE date >= ? AND date < ? AND (CAST(tempf AS REAL) - 32) * 5.0 / 9.0 BETWEEN 0 AND 7
        """, (date_str, next_day_str))
        count = cursor.fetchone()[0]
        return count * 5 / 3600

    def calculate_full_season_chill_hours(year):
        start_date = datetime(year - 1, 9, 1).strftime('%Y-%m-%d')
        end_date = datetime(year, 3, 1).strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT COUNT(*) FROM readings 
            WHERE date >= ? AND date < ? AND (CAST(tempf AS REAL) - 32) * 5.0 / 9.0 BETWEEN 0 AND 7
        """, (start_date, end_date))
        count = cursor.fetchone()[0]
        return count * 5 / 3600

    def calculate_daily_gdd(cursor, date):
        date_str = date.strftime('%Y-%m-%d')
        next_day_str = (date + timedelta(days=1)).strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT AVG(CAST(tempf AS REAL)) FROM readings 
            WHERE date >= ? AND date < ?
        """, (date_str, next_day_str))
        avg_temp_f = cursor.fetchone()[0] or 0
        avg_temp_c = (avg_temp_f - 32) * 5 / 9
        return max(0, avg_temp_c - 10)

    log("Precomputing historical average daily GDD.")
    historical_avg_gdd = []
    for target_doy in range(1, 367):
        total_gdd = 0
        count = 0
        for year in historical_years:
            try:
                date = datetime(year, 1, 1) + timedelta(days=target_doy - 1)
                if date.year == year:
                    total_gdd += calculate_daily_gdd(cursor, date)
                    count += 1
            except ValueError:
                continue
        avg_gdd = total_gdd / count if count > 0 else 0
        historical_avg_gdd.append(avg_gdd)
    log("Completed precomputing GDD.")

    cursor.execute("SELECT COUNT(*) FROM ehml_training_data")
    if cursor.fetchone()[0] == 0:
        historical_chill_hours = [calculate_full_season_chill_hours(year) for year in historical_years]
        current_year_chill_hours = np.mean(historical_chill_hours) if historical_chill_hours else 0
    else:
        current_year_chill_hours = 0
    log(f"Estimated current chill hours: {current_year_chill_hours:.2f}")

    log("Preparing training data.")
    X_train, y_train = [], []
    varieties_to_process = [(v, t) for v, t in varieties_data if t is not None]
    for variety, target_gdd in varieties_to_process:
        for year in historical_years:
            cursor.execute("""
                SELECT current_gdd, doy, chill_hours, mean_gdd, std_gdd, remaining_gdd 
                FROM ehml_training_data 
                WHERE variety = ? AND year = ?
            """, (variety, year))
            row = cursor.fetchone()
            if row:
                current_gdd, doy, chill_hours, mean_gdd, std_gdd, remaining_gdd = row
                X_train.append([current_gdd, doy, chill_hours, mean_gdd, std_gdd, target_gdd])
                y_train.append(remaining_gdd)
                continue

            cursor.execute("""
                SELECT date, gdd FROM readings 
                WHERE substr(date, 1, 4)=? AND gdd >= ? 
                ORDER BY date ASC LIMIT 1
            """, (str(year), target_gdd))
            row = cursor.fetchone()
            if not row:
                continue
            bud_break_date = datetime.fromisoformat(row[0].rstrip("Z"))
            bud_break_gdd = row[1]

            past_date = datetime(year, current_date.month, current_date.day)
            next_day = past_date + timedelta(days=1)
            cursor.execute(
                "SELECT MAX(gdd) FROM readings WHERE substr(date, 1, 4)=? AND date < ?",
                (str(year), next_day.isoformat())
            )
            current_gdd = cursor.fetchone()[0] or 0

            chill_hours = calculate_full_season_chill_hours(year)

            historical_gdds = [
                cursor.execute(
                    "SELECT MAX(gdd) FROM readings WHERE substr(date, 1, 4)=? AND date < ?",
                    (str(h_year), (datetime(h_year, current_date.month, current_date.day) + timedelta(days=1)).isoformat())
                ).fetchone()[0] or 0
                for h_year in historical_years
            ]
            mean_gdd = np.mean(historical_gdds)
            std_gdd = np.std(historical_gdds)

            features = [current_gdd, doy, chill_hours, mean_gdd, std_gdd, target_gdd]
            remaining_gdd = bud_break_gdd - current_gdd
            X_train.append(features)
            y_train.append(remaining_gdd)

            cursor.execute("""
                INSERT OR REPLACE INTO ehml_training_data 
                (variety, year, current_gdd, doy, chill_hours, mean_gdd, std_gdd, remaining_gdd)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (variety, year, current_gdd, doy, chill_hours, mean_gdd, std_gdd, remaining_gdd))
            conn.commit()

    model_file = "ehml_model.pkl"
    if os.path.exists(model_file):
        with open(model_file, 'rb') as f:
            model = pickle.load(f)
    else:
        if not X_train:
            log("Error: No training data.")
            return
        X_train = np.array(X_train)
        y_train = np.array(y_train)
        model = xgb.XGBRegressor(objective='reg:squarederror', n_estimators=50, max_depth=3)
        model.fit(X_train, y_train)
        with open(model_file, 'wb') as f:
            pickle.dump(model, f)
        cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring='neg_mean_squared_error')
        log(f"Cross-validation MSE: {-np.mean(cv_scores):.2f}")

    log("Starting predictions.")
    for variety, target_gdd in varieties_to_process:
        cursor.execute(f"SELECT {column_name} FROM grapevine_gdd WHERE variety = ?", (variety,))
        if cursor.fetchone()[0]:
            continue

        next_day = current_date + timedelta(days=1)
        cursor.execute(
            "SELECT MAX(gdd) FROM readings WHERE substr(date, 1, 4)=? AND date < ?",
            (str(current_year), next_day.isoformat())
        )
        current_gdd = cursor.fetchone()[0] or 0
        chill_hours = current_year_chill_hours
        historical_gdds = [
            cursor.execute(
                "SELECT MAX(gdd) FROM readings WHERE substr(date, 1, 4)=? AND date < ?",
                (str(h_year), (datetime(h_year, current_date.month, current_date.day) + timedelta(days=1)).isoformat())
            ).fetchone()[0] or 0
            for h_year in historical_years
        ]
        mean_gdd = np.mean(historical_gdds)
        std_gdd = np.std(historical_gdds)
        features = [current_gdd, doy, chill_hours, mean_gdd, std_gdd, target_gdd]

        remaining_gdd = model.predict(np.array([features]))[0]
        log(f"{variety} - Predicted remaining_gdd: {remaining_gdd:.2f}")

        accumulated_gdd = 0
        days_remaining = 0
        while accumulated_gdd < remaining_gdd and days_remaining < 365:
            next_doy = (doy + days_remaining - 1) % 366
            daily_gdd = historical_avg_gdd[next_doy] or 0.1
            accumulated_gdd += daily_gdd
            days_remaining += 1
        log(f"{variety} - Days remaining: {days_remaining}")

        predicted_date = current_date + timedelta(days=days_remaining)
        cursor.execute(f"UPDATE grapevine_gdd SET {column_name} = ? WHERE variety = ?",
                       (predicted_date.isoformat(), variety))
        log(f"{variety} - Predicted bud break: {predicted_date.isoformat()}")
        conn.commit()

    log("EHML projection completed.")

# --- Main Data Ingestion Loop ---
def main() -> None:
    """
    Main execution function for data retrieval pipeline.
    """
    reload_config()
    ensure_database_exists()
    conn = get_db_connection()
    cursor = conn.cursor()
    create_tables(conn, cursor)
    import_grapevine_csv(cursor, conn)
    import_vineyard_pests(cursor, conn)
    import_sunspots_data(cursor, conn)

    new_total = 0
    day = START_DATE.date()
    while day < CURRENT_DATE:
        reload_config()  # Reload configuration for each day iteration
        day_str = day.strftime("%Y-%m-%d")
        try:
            cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=?", (day_str,))
            old_count = cursor.fetchone()[0]
        except sqlite3.Error as e:
            log(f"Error fetching count for {day_str}: {e}")
            old_count = 0

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
                    values = {key: reading.get(key, None) for key in FIELDS_ORDER}
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
                        ({', '.join(FIELDS_ORDER)}, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                        VALUES ({', '.join(['?'] * len(FIELDS_ORDER))}, ?, ?, ?, ?, ?)
                    """
                    try:
                        insert_tuple = tuple(values.get(k, None) for k in FIELDS_ORDER) + (0, 0, 0, 0, MAC_ADDRESS)
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
                    backup_values = {key: backup_reading.get(key, None) for key in FIELDS_ORDER}
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
                            ({', '.join(FIELDS_ORDER)}, gdd, gdd_hourly, gdd_daily, is_generated, mac_source)
                            VALUES ({', '.join(['?'] * len(FIELDS_ORDER))}, ?, ?, ?, ?, ?)
                        """
                        try:
                            insert_tuple = tuple(backup_values.get(k, None) for k in FIELDS_ORDER) + (
                            0, 0, 0, 0, BACKUP_MAC_ADDRESS)
                            cursor.execute(sql, insert_tuple)
                            log(f"Inserted backup reading for {raw_date} from backup station.")
                        except Exception as ex:
                            log(f"Error inserting backup reading for {raw_date}: {ex}")
                    else:
                        if existing[0] is None:
                            update_clause = ", ".join([f"{col} = ?" for col in FIELDS_ORDER])
                            update_values = [backup_values.get(col, None) for col in FIELDS_ORDER]
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
                    cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=? AND tempf IS NOT NULL",
                                   (day_str,))
                    valid_count = cursor.fetchone()[0]
                except sqlite3.Error as e:
                    log(f"Error fetching valid count after backup for {day_str}: {e}")
            else:
                log(f"No backup data received for {day_str}.")

        if valid_count < 287:
            log(f"{day_str}: Only {valid_count} valid readings after primary/backup. Trying Open-Meteo historical.")
            om_inserted = insert_openmeteo_historical(cursor, conn, day_str)
            if om_inserted > 0:
                try:
                    cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=? AND tempf IS NOT NULL",
                                   (day_str,))
                    valid_count = cursor.fetchone()[0]
                except sqlite3.Error as e:
                    log(f"Error fetching valid count after Open-Meteo for {day_str}: {e}")
        if valid_count < 287:
            log(f"{day_str}: Only {valid_count} valid readings after all fallbacks. Filling gaps via interpolation.")
            fill_missing_data_by_gap(cursor, conn, day_str)
        else:
            log(f"{day_str}: All intervals have valid temperature data.")
        try:
            cursor.execute("SELECT COUNT(*) FROM readings WHERE substr(date,1,10)=?", (day_str,))
            new_count = cursor.fetchone()[0]
            log(f"After interpolation, {day_str} has {new_count} readings.")
        except sqlite3.Error as e:
            log(f"Error fetching count after interpolation for {day_str}: {e}")
        day += timedelta(days=1)

    recalc_gdd(cursor, conn, full=False)
    append_forecast_data(cursor, conn)
    log("Clearing all GDD values before full recalculation...")
    execute_sql(cursor, "UPDATE readings SET gdd = 0, gdd_hourly = 0, gdd_daily = 0", ())
    conn.commit()
    log("Performing final full recalculation of cumulative, hourly, and daily GDD...")
    recalc_gdd(cursor, conn, full=True)
    # Recalculate varietal-specific GDD using biofix_date
    recalc_varietal_gdd(cursor, conn)
    project_bud_break_regression(cursor, conn)
    project_bud_break_hybrid(cursor, conn)
    project_bud_break_ehml(cursor, conn)
    log("Data retrieval complete.")
    conn.close()


if __name__ == "__main__":
    main()
