import redis
import json


def clear_redis_data(redis_db: redis.StrictRedis):
    redis_db.flushdb()


def get_redis_data(redis_db: redis.StrictRedis, q_name):
    queue_data = redis_db.lrange(q_name, 0, -1)
    to_return = []
    for single_data in queue_data:
        to_return.append(json.loads(single_data))
    return to_return


def add_to_queue(redis_db: redis.StrictRedis, q_name, data):
    redis_db.rpush(q_name, json.dumps(data))



#
# from wtiproj04.dict_json_utils import *
# from copy import deepcopy
# import json
# redis_ratings = redis.StrictRedis(port=6381, host="localhost", db=2, charset="utf-8", decode_responses=True)
# redis_profile = redis.StrictRedis(port=6381, host="localhost", db=2, charset="utf-8", decode_responses=True)
# # cli.set("q", "blala")
# REDIS_RATINGS_TITLE = "ratings"
# REDIS_PROFILE_TITLE = "prof"
#
#
# merged_table, genres_cols = merge_data_frames(100)
# rows = get_json_rows(merged_table)
#
# for row in rows:
#     add_to_queue(redis_ratings, REDIS_RATINGS_TITLE, row)
#
#
# data = get_redis_data(redis_ratings, REDIS_RATINGS_TITLE)
# merged_table = convert_list_of_dcits_to_dataframe(deepcopy(data))
# _, avg_genre_rating = get_avg_genre_rating(merged_table, genres_cols, difference=False)
# avg_genre_rating = deepcopy(avg_genre_rating)
# redis_profile.set(REDIS_PROFILE_TITLE, json.dumps(list(avg_genre_rating)))
# print(redis_profile.get(REDIS_PROFILE_TITLE))