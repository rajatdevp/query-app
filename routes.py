from flask import Flask, request, jsonify, Blueprint
from services import QueryService

app = Blueprint('app', __name__)

@app.route('/api/query', methods=['GET', 'POST'])
def manage_users():
    if request.method == 'GET':
        users = QueryService.get_all_users()
        return users

    if request.method == 'POST':
        #                                                                            data = request.get_json()
        new_user = QueryService.get_all_users()
        return jsonify({'id': new_user, 'name': new_user, 'email': new_user}), 201