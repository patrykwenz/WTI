from flask import Flask, jsonify, abort, request
from zad15 import ElasticClient
import json
import numpy as np

app = Flask(__name__)
es = ElasticClient()


# es.index_documents()


class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """

    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32,
                              np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


# ------ Simple operations ------
@app.route("/user/document/<int:userid>", methods=["GET"])
def get_user(userid):
    try:
        index = request.args.get('index', default='users')
        result = es.get_movies_liked_by_user(userid, index)
        return jsonify(result)
    except:
        abort(404)


@app.route("/movie/document/<int:movieid>", methods=["GET"])
def get_movie(movieid):
    try:
        index = request.args.get('index', default='movies')
        result = es.get_users_that_like_movie(movieid, index)
        return jsonify(result)
    except:
        abort(404)


@app.route("/user/preselection/<int:userid>", methods=["GET"])
def use_preselection(userid):
    try:
        index = request.args.get('index', default='users')
        result = es.collab_user_filter(int(userid), index)
        result = {
            "moviesFound": result
        }
        return json.dumps(result, cls=NumpyEncoder)
    except:
        abort(404)


@app.route("/movie/preselection/<int:movieid>", methods=["GET"])
def movies_preselection(movieid):
    try:
        index = request.args.get('index', default='movies')
        result = es.collab_movie_filter(int(movieid), index)
        result = {
            "usersFound": result
        }
        return json.dumps(result, cls=NumpyEncoder)
    except:
        abort(404)


# ------ Add/Update/Delete ------
@app.route("/user/document/<int:user_id>", methods=["PUT"])
def add_user_document(user_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        movies_liked_by_user = request.json
        es.add_user(user_id, movies_liked_by_user, user_index=user_index, movie_index=movie_index)
        return "Ok", 200
    except:
        abort(400)


@app.route("/movie/document/<int:movie_id>", methods=["PUT"])
def add_movie_document(movie_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        users_who_like_movie = request.json
        es.add_movie(movie_id, users_who_like_movie, movie_index=movie_index, user_index=user_index)
        return "Ok", 200
    except:
        abort(400)


@app.route("/user/document/<int:user_id>", methods=["POST"])
def update_user_document(user_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        movies_liked_by_user = request.json
        es.up_user(user_id, list(movies_liked_by_user), user_index=user_index, movie_index=movie_index)
        return "Ok", 200
    except:
        abort(400)


@app.route("/movie/document/<int:movie_id>", methods=["POST"])
def update_movie_document(movie_id):
    try:
        user_index = request.args.get('user_index', default='users')
        movie_index = request.args.get('movie_index', default='movies')
        users_who_like_movie = request.json
        es.up_movie(movie_id, users_who_like_movie, user_index=user_index, movie_index=movie_index)
        return "Ok", 200
    except:
        abort(400)


@app.route("/user/document/<int:user_id>", methods=["DELETE"])
def delete_user_document(user_id):
    try:
        es.delete_user(user_id)
        return "Ok", 200
    except:
        abort(400)


@app.route("/movie/document/<int:movie_id>", methods=["DELETE"])
def delete_movie_document(movie_id):
    try:
        es.delete_movie(movie_id)
        return "Ok", 200
    except:
        abort(400)


@app.route("/user/bulk", methods=["POST"])
def bulk_update_users():
    """
    Body should look like this: [{"user_id": 123, "liked_movies": [1,2,3,4]}, ...]
    """
    print('User bulk')
    index = request.args.get('index', default='users')
    body = request.json
    print('body: {}'.format(body))
    print('index: {}'.format(index))
    es.bulk_user_update(body)
    return 'Ok', 200


@app.route("/movie/bulk", methods=["POST"])
def bulk_update_movies():
    """
    Body should look like this: [{"movie_id": 123, "users_who_liked_movie": [1,2,3,4]}, ...]
    """
    index = request.args.get('index', default='movies')
    body = request.json
    es.bulk_movie_update(body)
    return 'Ok', 200


@app.route("/index/<string:index_name>", methods=["DELETE"])
def del_ix(index_name):
    try:
        es.delete_index(index_name=str(index_name))
        return "Ok", 200
    except:
        abort(400)


@app.route("/index/<string:index_name>", methods=["PUT"])
def crt_ix(index_name):
    try:
        es.create_index(index_name=str(index_name))
        return "Ok", 200
    except:
        abort(400)


@app.route("/indexes", methods=["GET"])
def get_indexes():
    try:
        indexes = es.get_all_indexes()
        return json.dumps(indexes), 200
    except:
        abort(400)


@app.route("/reindex", methods=["POST"])
def rename_indexes():
    try:
        body = request.json
        src = body["src"]
        dest = body["dest"]
        es.reindex(src, dest)
        return "OK", 200
    except:
        abort(400)


if __name__ == '__main__':
    # es.index_documents()
    app.run(debug=True)
