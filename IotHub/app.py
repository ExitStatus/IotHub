from flask import Flask, request, jsonify, abort
from datetime import datetime, timezone
import os
import sqlite3

app = Flask(__name__)

# Make the WSGI interface available at the top level so wfastcgi can get it.
wsgi_app = app.wsgi_app

db_filename = 'iot.db'
data = {}

@app.route('/api/record', methods=['POST'])
def record_data():
    print("API /api/record called")
    print(request.json)
    if not request.json or "name" not in request.json or "power" not in request.json or "battery" not in request.json:
        abort(400)

    name = request.json.get('name')
    power = request.json.get('power')
    battery = request.json.get('battery')

    if not name in data:
        data[name] = {}

    data[name]["power"] = power;
    data[name]["battery"] = battery;

    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()

        update_current(cursor, name, "power", power)
        update_current(cursor, name, "battery", battery)

        conn.commit()
        cursor.close()
    except sqlite3.Error as error:
        print(error)
        abort(500)
    finally:
       if (conn):
            conn.close()

    return jsonify({'result': 'OK'})

def update_current(cursor, name, sensor, value):
    retval = cursor.execute("SELECT count(*) current_data WHERE client_name=? AND sensor_name=?", (name, sensor))
    values = retval.fetchone()
    if values[0] > 0:
        cursor.execute("UPDATE current_data SET sensor_value=?, last_reading_date=? WHERE client_name=? AND sensor_name=?", (value, datetime.now(timezone.utc), name, sensor))
    else:
        cursor.execute("INSERT INTO current_data (client_name, sensor_name, sensor_value, last_reading_date) VALUES (?, ?, ?, ?);", (name, sensor, value, datetime.now(timezone.utc)))


def create_database():
    try:
        conn = sqlite3.connect(db_filename)
        cursor = conn.cursor()

        cursor.execute('CREATE TABLE current_data ([client_name] text, [sensor_name] text, [sensor_value] integer, [last_reading_date] date, PRIMARY KEY (client_name, sensor_name))')
        cursor.execute('CREATE TABLE historical_data ([data_id] INTEGER PRIMARY KEY AUTOINCREMENT, [reading_date] date, [client_name] text, [sensor_name] text, [sensor_value] integer)')
        conn.commit()
        cursor.close()
    except sqlite3.Error as error:
        print(error)
    finally:
       if (conn):
            conn.close()

if not os.path.exists(db_filename):
    create_database()

if __name__ == '__main__':
    app.run('0.0.0.0', 5555)
