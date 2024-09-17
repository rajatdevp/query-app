from flask import Flask, request, jsonify, Blueprint
from services import QueryService

app = Blueprint('app', __name__)

@app.route('/api/query/<string:input_query>', methods=['GET', 'POST'])
def manage_users(input_query):
    if request.method == 'GET':
        print(input_query)
        users = QueryService.get_all_users()
        return users
    if request.method == 'POST':
        #                                                                            data = request.get_json()
        new_user = QueryService.get_all_users()
        return jsonify({'id': new_user, 'name': new_user, 'email': new_user}), 201


@app.route('/api/query/kafka', methods=['GET', 'POST'])
def manage_users_kafka():
    if request.method == 'GET':
        users = QueryService.get_all_users_kafka()
        return users
    if request.method == 'POST':
        #                                                                            data = request.get_json()
        new_user = QueryService.get_all_users()
        return jsonify({'id': new_user, 'name': new_user, 'email': new_user}), 201