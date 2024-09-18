from flask import Flask, request, jsonify, Blueprint

import services
from services import QueryService

app = Blueprint('app', __name__)

@app.route('/api/query/v1', methods=['GET', 'POST'])
def manage_users():
    if request.method == 'GET':
        users = QueryService.get_all_users()
        return users
    if request.method == 'POST':
        data = request.get_json()  # Parse the JSON data from the request body
        if not data:
            return jsonify({"error": "Invalid JSON data"}), 400

        # Access elements from the JSON data
        question = data.get('question')
        original_query = data.get('original_query')
        dataset = data.get('dataset')
        if original_query:
            follow_up_flag = 'Y'
        else:
            follow_up_flag = 'N'
        # Assuming you have a method to create a new user
        response_text = services.get_llm_response(question, follow_up_flag, original_query, dataset)
        blocks = services.extract_code_blocks(response_text)
        result_df = services.getData(blocks[0])
        result_json = services.df_to_json(result_df)
        graph_json = None
        exec(blocks[1])
        print("block 1: " + blocks[1])
        print("graph_json"+graph_json)
        answer_text = None
        exec(blocks[2])
        print("block 2: " + blocks[2])
        print("answer_text"+answer_text)
        return jsonify({'query': blocks[0],'table_data': result_json, 'graph_json': graph_json, 'answer_text': answer_text}), 201


@app.route('/api/query/kafka', methods=['GET', 'POST'])
def manage_users_kafka():
    if request.method == 'GET':
        users = QueryService.get_all_users_kafka()
        return users
    if request.method == 'POST':
        #data = request.get_json()
        new_user = QueryService.get_all_users()
        return jsonify({'id': new_user, 'name': new_user, 'email': new_user}), 201