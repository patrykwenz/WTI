from wtiproj03.merge_utils import get_json_rows, get_labels_names
from collections import ChainMap
from flask import Flask, jsonify, abort, request
import random


class RestLogic():
    def __init__(self):
        # self.data = get_json_rows()
        self.data = []
        self.avg_all_users_ratings = dict()
        self.avg_user_rating = dict()

    def generate_all_users_random_ratings(self):
        self.avg_all_users_ratings = dict(ChainMap(*[{str(name): float(random.randint(70, 100) / 100)} for name in
                                                     get_labels_names()]))

    def generate_avg_user_random_rating(self):
        avg_user_rating = []
        for user_info in self.data:
            dummy_data = dict()
            user_id = user_info["userID"]
            dummy_data["userID"] = str(user_id)
            dummy_data.update(
                dict(ChainMap(*[{str(name): float(random.randint(70, 100) / 100)} for name in
                                get_labels_names()]))
            )
            avg_user_rating.append(dummy_data)
        self.avg_user_rating = avg_user_rating

    def add_new_rating(self, new_rating):
        self.data.append(new_rating)

    def clear_data(self):
        self.data = []

    def get_json_data(self):
        return self.data[:100]

    def get_user_avg_rating(self, id):
        for rating in self.avg_user_rating:
            if rating["userID"] == id:
                return rating

    def get_all_users_avg_ratings(self):
        return self.avg_all_users_ratings


app = Flask(__name__)
rest_logic = RestLogic()


@app.route('/')
def index():
    return jsonify({"Title": "WTI PROJECT"}), 201


@app.route('/rating', methods=['POST'])
def add_rating():
    if request.method == "POST":
        some_rating = request.get_json()
        rest_logic.add_new_rating(some_rating)
        return jsonify(some_rating), 201


@app.route('/ratings', methods=['GET', 'DELETE'])
def get_ratings():
    if request.method == "GET":
        return jsonify(rest_logic.get_json_data())
    if request.method == "DELETE":
        rest_logic.clear_data()
        return jsonify(""), 204


@app.route('/avg-genre-ratings/<userID>', methods=['GET'])
def get_user_avg_rating(userID):
    rest_logic.generate_avg_user_random_rating()
    return jsonify(rest_logic.get_user_avg_rating(userID))


@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def get_all_users_avg_ratings():
    rest_logic.generate_all_users_random_ratings()
    return jsonify(rest_logic.get_all_users_avg_ratings())


if __name__ == '__main__':
    app.run(debug=True)
