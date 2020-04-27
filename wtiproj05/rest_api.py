from wtiproj03.merge_utils import get_json_rows, get_labels_names
from collections import ChainMap
from flask import Flask, jsonify, abort, request
from wtiproj04.dict_json_utils import *
import random
from copy import deepcopy
import redis
import json
from wtiproj05.redis_client import *


class RestLogic():
    def __init__(self, genres_cols):
        self.data = []
        self.genres_cols = genres_cols
        self.avg_users_rating = dict()
        self.user_profile = dict()
        self.redis_ratings = redis.StrictRedis(host="localhost", charset="utf-8", port=6381, db=2)
        self.redis_profile = redis.StrictRedis(host="localhost", charset="utf-8", port=6381, db=1)
        clear_redis_data(self.redis_ratings)
        clear_redis_data(self.redis_profile)
        self.REDIS_RATINGS_TITLE = "ratings"
        self.REDIS_PROFILE_TITLE = "AVG_GENRE_RATINGS"
        self.REDIS_SINGLE_USER_TITLE = "SINGLE_USER_RATING-"

    def add_new_rating(self, new_rating):
        add_to_queue(self.redis_ratings, self.REDIS_RATINGS_TITLE, data=new_rating)
        self.data = get_redis_data(self.redis_ratings, self.REDIS_RATINGS_TITLE)

    def clear_data(self):
        self.data = []
        clear_redis_data(self.redis_ratings)

    def get_data(self):
        self.data = get_redis_data(self.redis_ratings, self.REDIS_RATINGS_TITLE)
        return self.data

    def get_avg_genre_rating(self):
        return self.avg_genre_rating

    def get_avg_user_rating(self, id):
        return self.avg_users_rating[id]

    def create_avg_genre_rating(self):
        self.data = get_redis_data(self.redis_ratings, self.REDIS_RATINGS_TITLE)
        self.merged_table = convert_list_of_dcits_to_dataframe(deepcopy(self.data))
        _, avg_genre_rating = get_avg_genre_rating(self.merged_table, self.genres_cols, difference=False)
        self.avg_genre_rating = deepcopy(avg_genre_rating)
        self.redis_profile.set(self.REDIS_PROFILE_TITLE, json.dumps(list(self.avg_genre_rating)))
        return self.avg_genre_rating

    def create_avg_genre_rating_user(self, id):
        self.data = get_redis_data(self.redis_ratings, self.REDIS_RATINGS_TITLE)
        self.merged_table = convert_list_of_dcits_to_dataframe(deepcopy(self.data))
        single_user_ratings = get_single_user_ratings(self.merged_table, self.genres_cols, id)
        self.avg_users_rating[id] = single_user_ratings
        temp_title = self.REDIS_SINGLE_USER_TITLE + str(id)
        self.redis_profile.set(temp_title, json.dumps(list(self.avg_users_rating[id])))
        return self.avg_users_rating[id]

    def create_user_profile(self, id):
        self.create_avg_genre_rating()
        self.create_avg_genre_rating_user(id)
        self.avg_users_rating[id] = np.array(json.loads(self.redis_profile.get(
            self.REDIS_SINGLE_USER_TITLE + str(id)
        )))
        self.avg_genre_rating = np.array(json.loads(self.redis_profile.get(self.REDIS_RATINGS_TITLE)))
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


@app.route('/profile/<int:userID>', methods=['GET'])
def get_user_profile(userID):
    user_profile = rest_logic.create_user_profile(userID)
    return jsonify(list(json.dumps(user_profile)))


if __name__ == '__main__':
    app.run(debug=True)
