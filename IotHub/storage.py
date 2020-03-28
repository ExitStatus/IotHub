import os

import sqlite3
from datetime import datetime, timezone

db_filename = 'iot.db'

def create_database():
    if not os.path.exists(db_filename):
        try:
            conn = sqlite3.connect(db_filename)
            cursor = conn.cursor()

            cursor.execute('CREATE TABLE client ([client_name] text, [power_type] text, [power_level] integer, [last_reading_date] date, PRIMARY KEY (client_name))')
            cursor.execute('CREATE TABLE sensor_data ([client_name] text, [sensor_name] text, [sensor_value] integer, [last_reading_date] date, PRIMARY KEY (client_name, sensor_name))')
            cursor.execute('CREATE TABLE historical_data ([data_id] INTEGER PRIMARY KEY AUTOINCREMENT, [reading_date] date, [client_name] text, [sensor_name] text, [sensor_value] integer)')

            conn.commit()
            cursor.close()
        except sqlite3.Error as error:
            print(error)
        finally:
           if (conn):
                conn.close()

def update_client_stats(cursor, dateNow, client_name, power_type, power_level):
    retval = cursor.execute("SELECT count(*) FROM client WHERE client_name=?", [client_name])
    values = retval.fetchone()
    if values[0] > 0:
        cursor.execute("UPDATE client SET power_type=?, power_level=?, last_reading_date=? WHERE client_name=?", [power_type, power_level, dateNow, client_name])
        print(f"Updated client {client_name} with {power_type} = {power_level}")
    else:
        cursor.execute("INSERT INTO client (client_name, power_type, power_level, last_reading_date) VALUES (?,?,?,?)", [client_name, power_type, power_level, dateNow])
        print(f"Created client {client_name} with {power_type} = {power_level}")

    cursor.execute("INSERT INTO historical_data (reading_date, client_name, sensor_name, sensor_value) VALUES (?,?,?,?)", [dateNow, client_name, 'power_type', power_type])
    cursor.execute("INSERT INTO historical_data (reading_date, client_name, sensor_name, sensor_value) VALUES (?,?,?,?)", [dateNow, client_name, 'power_level', power_level])


def store_current_sensor(cursor, dateNow, name, sensor, value):
    retval = cursor.execute("SELECT count(*) FROM sensor_data WHERE client_name=? AND sensor_name=?", [name, sensor])
    values = retval.fetchone()

    if values[0] > 0:
        cursor.execute("UPDATE sensor_data SET sensor_value=?, last_reading_date=? WHERE client_name=? AND sensor_name=?", [value, dateNow, name, sensor])
        print(f"Updated client {name} with sensor {sensor} = {value}")
    else:
        cursor.execute("INSERT INTO sensor_data (client_name, sensor_name, sensor_value, last_reading_date) VALUES (?, ?, ?, ?);", [name, sensor, value, dateNow])
        print(f"Added client {name} with sensor {sensor} = {value}")

    cursor.execute("INSERT INTO historical_data (reading_date, client_name, sensor_name, sensor_value) VALUES (?,?,?,?)", [dateNow, name, sensor, value])


def record_client_data(name, power, powerlevel, sensors):
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()

        dateNow = datetime.now(timezone.utc)

        update_client_stats(cursor, dateNow, name, power, powerlevel)

        for sensor in sensors:
            store_current_sensor(cursor, dateNow, name, sensor, sensors[sensor])

        conn.commit()
        cursor.close()
    except sqlite3.Error as error:
        print(error)
    finally:
       if (conn):
            conn.close()

def get_client_data(client_name):
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()

        retval = cursor.execute("SELECT power_type, power_level, last_reading_date FROM client WHERE client_name=?", [client_name])
        values = retval.fetchone()
        if values is None or len(values) == 0:
            return None

        return {
            'client_name': client_name,
            'power_type': values[0],
            'power_level': values[1]
        }

        cursor.close()
    except sqlite3.Error as error:
        print(error)
    finally:
       if (conn):
            conn.close()


