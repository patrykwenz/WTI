from flask import Flask, jsonify, abort, request
from flask_restful import Resource, Api
import wtiproj03.merge_dataframes as df

import random

app = Flask(__name__)


def generate_avg_user_rating():
    to_return = []
    for user_info in data:
        dummy_data = dict()
        user_id = user_info["userID"]
        dummy_data["userID"] = str(user_id)
        for name in df.get_genres_names():
            dummy_data[str("genre-" + name)] = float(random.randint(70, 100) / 100)
        to_return.append(dummy_data)
    return to_return

#
# def test():
#     to_return = []
#     for id in range(50):
#         dummy_dict = dict()
#         dummy_dict["userID"] = str(id)
#         for name in df.get_genres_names():
#             dummy_dict[str("genre-" + name)] = float(random.randint(70, 100) / 100)
#         to_return.append(dummy_dict)
#     return to_return
#

avg_all_users_ratings = [{str("genre-" + name): float(random.randint(70, 100) / 100)} for name in df.get_genres_names()]
data = df.generate_json_data()
avg_user_rating = generate_avg_user_rating()


@app.route('/')
def index():
    return jsonify({"Title": "WTI PROJECT"}), 201


@app.route('/rating', methods=['POST'])
def add_rating():
    if request.method == "POST":
        some_rating = request.get_json()
        data.append(some_rating)
        return jsonify(some_rating), 201


@app.route('/ratings', methods=['GET', 'DELETE'])
def get_ratings():
    # if request.method == "POST":
    #     return jsonify({"You did it": "appended data"})
    if request.method == "GET":
        return jsonify(data)
    if request.method == "DELETE":
        data.clear()
        return jsonify({"You have deleted": "data"})


@app.route('/avg-genre-ratings/<string:userID>', methods=['GET'])
def get_user_avg_rating(userID):
    for user_info in avg_user_rating:
        if user_info["userID"] == userID:
            return user_info


@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def get_all_users_avg_ratings():
    return jsonify(avg_all_users_ratings), 201


@app.errorhandler(404)
def not_found(e):
    return '', 404


if __name__ == '__main__':
    app.run(debug=True)
