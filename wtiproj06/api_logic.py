from copy import deepcopy
import numpy as np
from flask import Flask, jsonify, request
import wtiproj06.cass_client as cc
import wtiproj06.utils as utils


class RestLogic():
    def __init__(self, genres_cols):
        self.data = []
        self.genres_cols = genres_cols
        self.avg_users_rating = dict()
        self.user_profile = dict()
        self.cass_client = cc.CassClient()
        self.cass_client.create_user_rated_movies_table()
        self.cass_client.create_avg_genre_ratings_table()

    def add_new_rating(self, new_rating):
        new_rating = utils.swap_nan_with_zero(new_rating)
        self.cass_client.push_to_table(new_rating)

    def get_ratings(self):
        self.data = self.cass_client.get_all_elements("user_rated_movies")
        return self.data

    def clear_data(self):
        self.data = []
        self.cass_client.clear_table("user_rated_movies")

    def create_avg_genre_rating(self):
        temp_data = self.cass_client.get_all_elements("user_rated_movies")
        self.merged_table = utils.convert_list_of_dcits_to_dataframe(deepcopy(temp_data))
        _, avg_genre_rating = utils.get_avg_genre_rating(self.merged_table, self.genres_cols, difference=False)
        self.avg_genre_rating = list(deepcopy(avg_genre_rating))
        return self.avg_genre_rating

    def create_avg_genre_rating_user(self, id):
        temp_data = self.cass_client.get_all_elements("user_rated_movies")
        print("temp",temp_data)
        self.merged_table = utils.convert_list_of_dcits_to_dataframe(deepcopy(temp_data))
        single_user_ratings = utils.get_single_user_ratings(self.merged_table, self.genres_cols, id)

        self.avg_users_rating[id] = single_user_ratings
        single_user_ratings = np.array(single_user_ratings)
        single_user_ratings = np.nan_to_num(single_user_ratings).tolist()

        self.cass_client.push_to_table(self.cass_client.edit_avg_rating(single_user_ratings, id), avg=True)
        return self.avg_users_rating[id]

    def create_user_profile(self, id):
        self.avg_genre_rating = np.array(self.create_avg_genre_rating())
        single_user_rating = self.create_avg_genre_rating_user(id)
        self.user_profile[id] = np.array(single_user_rating) - self.avg_genre_rating
        return np.nan_to_num(self.user_profile[id])


app = Flask(__name__)

merged_table, genres_cols = utils.merge_data_frames(100)
rows = utils.get_json_rows(merged_table)
rest_logic = RestLogic(genres_cols)
rest_logic.clear_data()

# for row in rows:
#     rest_logic.add_new_rating(row)


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
        data = rest_logic.get_ratings()
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
    return jsonify(list(user_profile))


if __name__ == '__main__':
    app.run(debug=True)
