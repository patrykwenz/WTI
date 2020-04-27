from wtiproj03.merge_utils import get_json_rows, get_labels_names
from collections import ChainMap
from flask import Flask, jsonify, abort, request
from wtiproj04.dict_json_utils import *
import random
from copy import deepcopy
import redis
import json


class RestLogic():
    def __init__(self, genres_cols):
        self.data = []
        self.genres_cols = genres_cols
        self.avg_users_rating = dict()
        self.user_profile = dict()

    def add_new_rating(self, new_rating):
        self.data.append(new_rating)

    def clear_data(self):
        self.data = []

    def get_data(self):
        return self.data

    def get_avg_genre_rating(self):
        return self.avg_genre_rating

    def get_avg_user_rating(self, id):
        return self.avg_users_rating[id]

    def create_avg_genre_rating(self):
        self.merged_table = convert_list_of_dcits_to_dataframe(deepcopy(self.data))
        _, avg_genre_rating = get_avg_genre_rating(self.merged_table, self.genres_cols, difference=False)
        self.avg_genre_rating = deepcopy(avg_genre_rating)
        return self.avg_genre_rating

    def create_avg_genre_rating_user(self, id):
        self.merged_table = convert_list_of_dcits_to_dataframe(deepcopy(self.data))
        single_user_ratings = get_single_user_ratings(self.merged_table, self.genres_cols, id)
        self.avg_users_rating[id] = single_user_ratings
        self.redis_user_title = "genre_ratings_user-"
        return self.avg_users_rating[id]

    def create_user_profile(self, id):
        self.create_avg_genre_rating()
        self.create_avg_genre_rating_user(id)
        self.user_profile[id] = self.avg_users_rating[id] - self.avg_genre_rating
        return self.user_profile[id]


app = Flask(__name__)

merged_table, genres_cols = merge_data_frames(2000)
rows = get_json_rows(merged_table)
rest_logic = RestLogic(genres_cols)

for row in rows:
    rest_logic.add_new_rating(row)

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
        data = rest_logic.get_data()
        return jsonify(data)
    if request.method == "DELETE":
        rest_logic.clear_data()
        return jsonify(""), 204


@app.route('/avg-genre-ratings/<int:userID>', methods=['GET'])
def get_user_avg_rating(userID):
    user_rating = rest_logic.create_avg_genre_rating_user(userID)
    return jsonify(list(user_rating))


@app.route('/avg-genre-ratings/all-users', methods=['GET'])
def get_all_users_avg_ratings():
    rating = rest_logic.create_avg_genre_rating()
    return jsonify(list(rating))


@app.route('/profile/<userID>', methods=['GET'])
def get_user_profile(userID):
    user_profile = rest_logic.create_user_profile(userID)
    return jsonify(list(user_profile))


if __name__ == '__main__':
    app.run(debug=True)
