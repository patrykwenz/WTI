import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np
import json


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
                    "_id": str(userID)
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
                            "_id": str(userID)
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
                    "_id": str(movieId)
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
                            "_id": str(movieId)
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


if __name__ == '__main__':
    ec = ElasticClient()
    print("User filter for no 75")
    print(ec.collab_user_filter(75))
    print("Movie filter for no 1")
    print(ec.collab_movie_filter(1))
