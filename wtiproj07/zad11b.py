import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np


class ElasticClient:
    def __init__(self, address='localhost:10000'):
        self.es = Elasticsearch(address)

    # ------ Simple operations ------
    def index_documents(self):
        df = pd.read_csv('data/user_ratedmovies', delimiter='\t').loc[:, ['userID', 'movieID', 'rating']]
        means = df.groupby(['userID'], as_index=False, sort=False).mean().loc[:, ['userID', 'rating']].rename(
            columns={'rating': 'ratingMean'})
        df = pd.merge(df, means, on='userID', how="left", sort=False)
        df['ratingNormal'] = df['rating'] - df['ratingMean']
        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']].rename(
            columns={'ratingNormal': 'rating'}).pivot_table(index='userID', columns='movieID', values='rating').fillna(
            0)

        print("Indexing users...")
        index_users = [{
            "_index": "users",
            "_type": "user",
            "_id": index,
            "_source": {
                'ratings': row[row > 0].sort_values(ascending=False).index.values.tolist()
            }
        } for index, row in ratings.iterrows()]
        helpers.bulk(self.es, index_users)
        print("Done")
        print("Indexing movies...")
        index_movies = [{
            "_index": "movies",
            "_type": "movie",
            "_id": column,
            "_source": {
                "whoRated": ratings[column][ratings[column] >
                                            0].sort_values(ascending=False).index.values.tolist()
            }
        } for column in ratings]
        helpers.bulk(self.es, index_movies)
        print("Done")

    def get_movies_liked_by_user(self, user_id, index='users'):
        user_id = int(user_id)
        return self.es.get(index=index, doc_type="user", id=user_id)["_source"]

    def get_users_that_like_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]

    def collab_user_filter(self, userID):
        q = {
            "query": {
                "match": {
                    "_id": int(userID)
                }
            }
        }

        val = self.es.search(index='users', body=q, filter_path=['hits.hits._source'])['hits']['hits']
        movieId = val[0]['_source']['ratings']

        newUsersQuery = {
            "query": {
                "bool": {
                    "must_not": {
                        "term": {
                            "_id": int(userID)
                        }
                    },
                    "filter": {
                        "terms": {"ratings": movieId}
                    }
                }
            }
        }
        val = self.es.search(index='users', body=newUsersQuery, filter_path=['hits.hits._source'], size=10000)
        all_hits = val["hits"]["hits"]

        collab_movie_ids = [movieid for hit in all_hits for movieid in hit['_source']['ratings']]

        return list(np.unique(collab_movie_ids))

    def collab_movie_filter(self, movieId):
        query = {
            "query": {
                "match": {
                    "_id": int(movieId)
                }
            }
        }

        val = self.es.search(index='movies', body=query, filter_path=['hits.hits._source'])['hits']['hits']
        movies = val[0]['_source']['whoRated']

        newMovieQuery = {
            "query": {
                "bool": {
                    "must_not": {
                        "term": {
                            "_id": int(movieId)
                        }
                    },
                    "filter": {
                        "terms": {"whoRated": movies}
                    }
                }
            }
        }
        val = self.es.search(index='movies', body=newMovieQuery, filter_path=['hits.hits._source'], size=10000)
        all_hits = val["hits"]["hits"]

        collab_user_ids = [user_id for hit in all_hits for user_id in hit['_source']['whoRated']]
        return list(np.unique(collab_user_ids))

    def _movie_helper_add(self, userId, ratings):
        for item in ratings:
            query = {
                "query": {
                    "match": {
                        "_id": int(item)
                    }
                }
            }
            val = self.es.search(index='movies', body=query, filter_path=['hits.hits._source'])

            if val == {}:
                content = {
                    'whoRated': [int(userId)]
                }

                self.es.index(index='movies', doc_type='movie', id=int(item), body=content)
            else:
                t = self.es.get(index='movies', doc_type='movie', id=item)['_source']['whoRated']
                t.append(userId)
                new_ratings = list(np.unique(t))
                request = {
                    'doc': {
                        'whoRated': new_ratings
                    }
                }
                self.es.update(index='movies', doc_type='movie', id=int(item), body=request)

    def _movie_helper_delete(self, userID, movies_to_remove):
        for m_rem in movies_to_remove:
            m_doc = self.es.get(index="movies", doc_type="movie", id=int(m_rem))
            movies = m_doc["_source"]["whoRated"]
            movies.remove(userID)
            movies = list(set(movies))
            q = {
                "doc": {
                    "whoRated": movies
                }
            }
            self.es.update(index="movies", doc_type="movie", id=int(m_rem), body=q)

    def add_user(self, userId, ratings):
        if not ratings:
            content = {
                'ratings': []
            }
            self.es.index(index='users', doc_type='user', id=int(userId), body=content)
        else:
            content = {
                'ratings': ratings
            }
            self.es.index(index='users', doc_type='user', id=int(userId), body=content)
            self._movie_helper_add(userId, ratings)

    def update_user(self, userId, ratings, add=True):
        query = {
            "query": {
                "match": {
                    "_id": int(userId)
                }
            }
        }
        val = self.es.search(index='users', body=query, filter_path=['hits.hits._source'])

        if val == {}:
            self.add_user(userId, ratings)
        else:

            tmp = val['hits']['hits'][0]['_source']['ratings']

            if add:
                new_ratings = list(set(ratings + tmp))
                content = {'doc': {
                    'ratings': new_ratings
                }}
                self.es.update(index='users', doc_type='user', id=int(userId), body=content)
                self._movie_helper_add(userId, ratings)
            if not add:
                for item in ratings:
                    if item in tmp:
                        tmp.remove(item)

                content = {'doc': {
                    'ratings': tmp
                }}
                self.es.update(index='users', doc_type='user', id=int(userId), body=content)
                self._movie_helper_delete(userId, ratings)

    def delete_user(self, userID):
        tmp = self.get_movies_liked_by_user(userID)['ratings']
        self._movie_helper_delete(userID, tmp)
        self.es.delete(index='users', doc_type='user', id=int(userID))
        print("User deleted")

    def _user_helper_add(self, movieID, whorated):
        for item in whorated:
            query = {
                "query": {
                    "match": {
                        "_id": int(item)
                    }
                }
            }
            val = self.es.search(index='users', body=query, filter_path=['hits.hits._source'])

            if val == {}:
                content = {
                    'ratings': [int(movieID)]
                }

                self.es.index(index='users', doc_type='user', id=int(item), body=content)
            else:
                t = self.es.get(index='users', doc_type='user', id=item)['_source']['ratings']
                t.append(movieID)
                new_ratings = list(np.unique(t))
                request = {
                    'doc': {
                        'ratings': new_ratings
                    }
                }
                self.es.update(index='users', doc_type='user', id=int(item), body=request)

    def _user_helper_delete(self, movieID, users_to_remove):
        for u_rem in users_to_remove:
            u_doc = self.es.get(index="users", doc_type="user", id=int(u_rem))
            users = u_doc["_source"]["ratings"]
            users.remove(movieID)
            users = list(set(users))
            q = {
                "doc": {
                    "ratings": users
                }
            }
            self.es.update(index="users", doc_type="user", id=int(u_rem), body=q)

    def add_movie(self, movieId, userId):
        if not userId:
            content = {
                'whoRated': []
            }
            self.es.index(index='movies', doc_type='movie', id=int(movieId), body=content)
        else:
            content = {
                'whoRated': userId
            }
            self.es.index(index='movies', doc_type='movie', id=int(movieId), body=content)
            self._user_helper_add(movieId, userId)

    def delete_movie(self, movieID):
        movieID = int(movieID)
        m_doc = self.es.get(index="movies", doc_type="movie", id=movieID)["_source"]["whoRated"]
        self._user_helper_delete(movieID, m_doc)
        self.es.delete(index="movies", doc_type="movie", id=int(movieID))
        print("deleted movie")

    def up_user(self, userID, ratings):
        userID = int(userID)
        ratings = list(set(ratings))
        u_doc = self.es.get(index="users", doc_type="user", id=int(userID))
        old_u_doc = u_doc["_source"]["ratings"]

        movies_to_add = np.setdiff1d(ratings, old_u_doc)
        movies_to_remove = np.setdiff1d(old_u_doc, ratings)

        self._movie_helper_delete(userID, movies_to_remove)
        for m_add in movies_to_add:
            q = {
                "query": {
                    "match": {
                        "_id": int(m_add)
                    }
                }
            }
            s = self.es.search(index='users', body=q, filter_path=['hits.hits._source'])

            if s == {}:
                self.add_movie(movieId=m_add, userId=[userID])

            m_doc = self.es.get(index="movies", doc_type="movie", id=int(m_add))
            movies = m_doc["_source"]["whoRated"]
            movies.append(userID)
            movies = list(set(movies))
            q = {
                "doc": {
                    "whoRated": movies
                }
            }
            self.es.update(index="movies", doc_type="movie", id=int(m_add), body=q)

        b = {
            "doc": {
                "ratings": ratings
            }
        }
        self.es.update(index="users", doc_type="user", id=int(userID), body=b)

    def up_movie(self, movieID, who_rated):
        movieID = int(movieID)
        who_rated = list(set(who_rated))
        u_doc = self.es.get(index="movies", doc_type="movie", id=int(movieID))
        old_u_doc = u_doc["_source"]["whoRated"]

        users_to_add = np.setdiff1d(who_rated, old_u_doc)
        users_to_remove = np.setdiff1d(old_u_doc, who_rated)

        self._user_helper_delete(movieID, users_to_remove)

        for u_add in users_to_add:
            u_doc = self.es.get(index="users", doc_type="user", id=int(u_add))
            users = u_doc["_source"]["ratings"]
            users.append(movieID)
            users = list(set(users))
            q = {
                "doc": {
                    "ratings": users
                }
            }
            self.es.update(index="users", doc_type="user", id=int(u_add), body=q)

        b = {
            "doc": {
                "ratings": who_rated
            }
        }
        self.es.update(index="movies", doc_type="movie", id=int(movieID), body=b)

    def bulk_user_update(self, data):
        for item in data:
            self.up_user(userID=item["user_id"], ratings=item["liked_movies"])

    def bulk_movie_update(self, data):
        for item in data:
            self.up_movie(movieID=item["movie_id"], who_rated=item["users_who_liked_movie"])



if __name__ == '__main__':
    ec = ElasticClient()
    # ec.index_documents()
    print("Add movie 6666")
    ec.add_movie(6666, [])
    print("Add movie 7777")
    ec.add_movie(7777, [])
    print("Who rated 6666:", ec.get_users_that_like_movie(6666))
    print("Who rated 7777:", ec.get_users_that_like_movie(7777))
    print("Add user 999999")
    ec.add_user(999999, [6666, 7777])
    print("Ratings for 999999:", ec.get_movies_liked_by_user(999999))
    print("Upload user")
    ec.up_user(999999, [6666])
    print("Ratings for 999999:", ec.get_movies_liked_by_user(999999))
    print("Delete movie 6666")
    ec.delete_movie(6666)
    print("Ratings for 999999:", ec.get_movies_liked_by_user(999999))
    print("Delete user 999999")
    ec.delete_user(999999)
    # print(ec.get_movies_liked_by_user(999999))
