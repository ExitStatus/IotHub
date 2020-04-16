import os

import sqlite3
from datetime import datetime, timezone
import logFactory

db_filename = 'iot.db'

def create_database():
    if not os.path.exists(db_filename):
        try:
            conn = sqlite3.connect(db_filename)
            cursor = conn.cursor()

            cursor.execute('CREATE TABLE client ([client_id] INTEGER PRIMARY KEY AUTOINCREMENT, [client_name] text, [power_type] text, [power_level] integer, [last_reading_date] date)')
            cursor.execute('CREATE TABLE sensor ([sensor_id] INTEGER PRIMARY KEY AUTOINCREMENT, [client_id] integer, [sensor_name] text, FOREIGN KEY(client_id) REFERENCES client(client_id))')
            cursor.execute('CREATE TABLE sensor_data ([sensor_id] integer primary key, [sensor_value] numeric, [last_reading_date] date, FOREIGN KEY(sensor_id) REFERENCES sensor(sensor_id))')

            cursor.execute('CREATE TABLE historical_data ([reading_date] date, [sensor_id] INTEGER, [sensor_value] numeric, FOREIGN KEY(sensor_id) REFERENCES sensor(sensor_id))')

            conn.commit()
            cursor.close()
        except sqlite3.Error as error:
            logFactory.GetLogger("app").error(error)
        finally:
           if (conn):
                conn.close()

# -------------
# Client access
# -------------
def get_client_id(cursor, client_name):
    query = cursor.execute("SELECT client_id FROM client WHERE client_name=?", [client_name])
    row = query.fetchone()
    if row is None:
        return None
    else:
        return row[0]

def create_client(cursor, dateNow, client_name, power_type, power_level):
    cursor.execute("INSERT INTO client (client_name, power_type, power_level, last_reading_date) VALUES (?,?,?,?)", [client_name, power_type, power_level, dateNow])
    return get_client_id(cursor, client_name)

def update_client(cursor, dateNow, client_id, power_type, power_level):
    cursor.execute("UPDATE client SET power_type=?, power_level=?, last_reading_date=? WHERE client_id=?", [power_type, power_level, dateNow, client_id])

# -------------
# Sensor access
# -------------
def get_sensor_id(cursor, client_id, sensor_name):
    query = cursor.execute("SELECT sensor_id FROM sensor WHERE client_id=? AND sensor_name=?", [client_id, sensor_name])
    row = query.fetchone()
    if row is None:
        return None
    else:
        return row[0]

def create_sensor(cursor, client_id, sensor_name):
    cursor.execute("INSERT INTO sensor (client_id, sensor_name) VALUES (?,?)", [client_id, sensor_name])
    return get_sensor_id(cursor, client_id, sensor_name)

# -----------
# Sensor data
# -----------
def sensor_data_exists(cursor, sensor_id):
    query = cursor.execute("SELECT COUNT(*) from sensor_data WHERE sensor_id=?", [sensor_id])
    row = query.fetchone()
    if row[0] > 0:
        return True
    else:
        return False

def create_sensor_data(cursor, dateNow, sensor_id, sensor_value):
    cursor.execute("INSERT INTO sensor_data (sensor_id, sensor_value, last_reading_date) VALUES (?,?,?)", [sensor_id, sensor_value, dateNow])

def update_sensor_data(cursor, dateNow, sensor_id, sensor_value):
    cursor.execute("UPDATE sensor_data SET sensor_value=?, last_reading_date=? WHERE sensor_id=?", [sensor_value, dateNow, sensor_id])

# ---------------
# Historical Data
# ---------------
def create_historical_data(cursor, dateNow, sensor_id, sensor_data):
    cursor.execute("INSERT INTO historical_data (reading_date, sensor_id, sensor_value) VALUES (?,?,?)", [dateNow, sensor_id, sensor_data])

def record_client_data(client_name, power, powerlevel, sensors):
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()

        dateNow = datetime.now(timezone.utc)

        client_id = get_client_id(cursor, client_name)
        if client_id is None:
            client_id = create_client(cursor, dateNow, client_name, power, powerlevel)
        else:
            update_client(cursor, dateNow, client_id, power,  powerlevel)

        if power == "DC":
            power_level_id = get_sensor_id(cursor, client_id, "power_level")
            if power_level_id is None:
                power_level_id = create_sensor(cursor, client_id, "power_level")

            create_historical_data(cursor,  dateNow, power_level_id, powerlevel)

        for sensor in sensors:
            sensor_id = get_sensor_id(cursor, client_id, sensor)
            if sensor_id is None:
                sensor_id = create_sensor(cursor, client_id, sensor)

            if not sensor_data_exists(cursor, sensor_id):
                create_sensor_data(cursor, dateNow, sensor_id, sensors[sensor])
            else:
                update_sensor_data(cursor, dateNow, sensor_id, sensors[sensor])

            create_historical_data(cursor,  dateNow, sensor_id, sensors[sensor])

        conn.commit()
        cursor.close()
    except sqlite3.Error as error:
        logFactory.GetLogger("app").error(error)
    except Exception:
        traceback.print_exc(file=sys.stdout)
    finally:
       if (conn):
            conn.close()

def get_client_data(client_name):
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()

        query = cursor.execute("SELECT client_id, power_type, power_level, last_reading_date FROM client WHERE client_name=?", [client_name])
        values = query.fetchone()
        if values is None or len(values) == 0:
            return None

        cursor.close()

        return {
            'client_id': values[0],
            'client_name': client_name,
            'power_type': values[1],
            'power_level': values[2]
        }

    except sqlite3.Error as error:
       logFactory.GetLogger("app").error(error)
    finally:
       if (conn):
            conn.close()

def get_all_clients():
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()

        clients = []
        query = cursor.execute("SELECT client_id, client_name, power_type, power_level, last_reading_date FROM client ORDER BY client_name")
        rows = query.fetchall()

        for row in rows:
            clients.append({
                'client_id': row[0],
                'client_name': row[1],
                'power_type': row[2],
                'power_level': row[3],
                'last_reading': row[4]
            })

        cursor.close()

        return clients;
    except sqlite3.Error as error:
       logFactory.GetLogger("app").error(error)
    finally:
       if (conn):
            conn.close()

def get_client_history(client_name, sensor_name):
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()

        values = []

        query = cursor.execute("SELECT reading_date, sensor_value FROM historical_data WHERE client_name=? AND sensor_name=?", [client_name, sensor_name])
        rows = query.fetchall()
        
        for row in rows:
            values.append({ 'reading_date': row[0], 'sensor_value': row[1] })

        cursor.close()

        return {
            'client_name': client_name,
            'sensor_name': sensor_name,
            'values': values
        }

    except sqlite3.Error as error:
       logFactory.GetLogger("app").error(error)
    finally:
       if (conn):
            conn.close()

