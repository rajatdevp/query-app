from flask import Flask, request, jsonify, Blueprint
from flask_cors import cross_origin

import services
from services import QueryService

app = Blueprint('app', __name__)

@app.route('/api/query/v1', methods=['GET', 'POST'])
@cross_origin(origin='*')
def manage_users():
    if request.method == 'GET':
        datasets = services.get_datasets()
        return datasets
    if request.method == 'POST':
        request_data = request.get_json()  # Parse the JSON data from the request body
        if not request_data:
            return jsonify({"error": "Invalid JSON data"}), 400
    return services.process_request(request_data)


@app.route('/api/query/kafka', methods=['GET', 'POST'])
def manage_users_kafka():
    if request.method == 'GET':
        users = QueryService.get_all_users_kafka()
        return users
    if request.method == 'POST':
        #data = request.get_json()
        new_user = QueryService.get_all_users()
        return jsonify({'id': new_user, 'name': new_user, 'email': new_user}), 201