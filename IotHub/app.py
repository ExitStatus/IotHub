from flask import Flask, request, jsonify, abort

import os
import storage

app = Flask(__name__)

# Make the WSGI interface available at the top level so wfastcgi can get it.
wsgi_app = app.wsgi_app



@app.route('/api/record', methods=['POST'])
def record_data():
    print("API /api/record called")
    print(request.json)
    if not request.json or "client_name" not in request.json or "power_type" not in request.json or "power_level" not in request.json:
        abort(400)
    
    name = request.json.get('client_name')
    power = request.json.get('power_type')
    battery = request.json.get('power_level')

    storage.record_client_data(name, power, battery, request.json['sensors'])

    return jsonify({'result': 'OK'})

@app.route('/api/clients', methods=['GET'])
def get_all_clients():
    obj = storage.get_all_clients();
    return jsonify(obj)

@app.route('/api/client/<client>', methods=['GET'])
def get_client_data(client):
    obj = storage.get_client_data(client);
    return jsonify(obj)

@app.route('/api/client/history/<client>/<sensor>', methods=['GET'])
def get_client_history(client, sensor):
    obj = storage.get_client_history(client, sensor);
    return jsonify(obj)

storage.create_database()

if __name__ == '__main__':
    app.run('0.0.0.0', 5555)
