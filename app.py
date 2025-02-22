from flask import Flask, jsonify, request
import psycopg2
import configparser
import sys

app = Flask(__name__)

# Load PostgreSQL connection settings from config.ini
config = configparser.ConfigParser()
config.read('config.ini')
try:
    pg_host = config.get('postgres', 'host')
    pg_port = config.getint('postgres', 'port')
    pg_db = config.get('postgres', 'dbname')
    pg_user = config.get('postgres', 'user')
    pg_pass = config.get('postgres', 'password')
except Exception as e:
    sys.exit(f"Error reading PostgreSQL settings: {e}")


def get_db_connection():
    """
    Establish and return a connection to the PostgreSQL database.
    """
    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            dbname=pg_db,
            user=pg_user,
            password=pg_pass
        )
        return conn
    except Exception as e:
        sys.exit(f"Database connection error: {e}")


@app.route('/api/readings', methods=['GET'])
def get_readings():
    """
    Retrieve a list of readings from the 'readings' table.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM readings ORDER BY dateutc ASC;")
        rows = cursor.fetchall()
        # Extract column names from the cursor description
        colnames = [desc[0] for desc in cursor.description]
        data = [dict(zip(colnames, row)) for row in rows]
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/grapevine', methods=['GET'])
def get_grapevine():
    """
    Retrieve all rows from the 'grapevine_gdd' table.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM grapevine_gdd;")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        data = [dict(zip(colnames, row)) for row in rows]
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/sunspots', methods=['GET'])
def get_sunspots():
    """
    Retrieve all sunspot records from the 'sunspots' table ordered by date.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM sunspots ORDER BY date ASC;")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        data = [dict(zip(colnames, row)) for row in rows]
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/vineyard-pests', methods=['GET'])
def get_vineyard_pests():
    """
    Retrieve all records from the 'vineyard_pests' table.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM vineyard_pests;")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        data = [dict(zip(colnames, row)) for row in rows]
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


@app.route('/api/data', methods=['GET'])
def get_all_data():
    """
    Aggregate data from readings, grapevine_gdd, sunspots, and vineyard_pests tables.
    This endpoint can be used by the client to load all relevant data in one call.
    """
    data = {}
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM readings ORDER BY dateutc ASC")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        data['readings'] = [dict(zip(colnames, row)) for row in rows]

        # Grapevine data
        cursor.execute("SELECT * FROM grapevine_gdd;")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        data['grapevine'] = [dict(zip(colnames, row)) for row in rows]

        # Sunspots data
        cursor.execute("SELECT * FROM sunspots ORDER BY date ASC;")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        data['sunspots'] = [dict(zip(colnames, row)) for row in rows]

        # Vineyard pests data
        cursor.execute("SELECT * FROM vineyard_pests;")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        data['vineyard_pests'] = [dict(zip(colnames, row)) for row in rows]

        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    # For development purposes; in production, use a proper WSGI server
    app.run(debug=True)