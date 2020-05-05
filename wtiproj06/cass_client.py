from cassandra.cluster import Cluster
from cassandra.query import dict_factory
import wtiproj06.utils
import json

KEYSPACE = "ratings_keyspace"
USER_TABLE = "user_rated_movies"
AVG_RATINGS = "avg_genre_ratings_for_user"


class CassClient:
    def __init__(self):
        self.cluster = Cluster(["127.0.0.1"], port=9042)
        self.session = self.cluster.connect()
        self._create_ks(KEYSPACE)
        self.session.set_keyspace(KEYSPACE)
        self.session.row_factory = dict_factory

    def _create_ks(self, keyspace):
        self.session.execute("""
           CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
           WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
           """)

    def create_table(self, keyspace, table, pk, cols):
        cols = [f"{key} {val}, " for key, val in cols.items()]
        cols = "".join(cols)
        return """
                 CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + \
               table + """ ( """ + cols + \
               """PRIMARY KEY(""" + pk + """))
               """

    def delete_table(self, keyspace, table_name):
        self.session.execute(f"DROP TABLE {keyspace}.{table_name}")

    def set_key_and_type(self, table_name):
        _, cols_names = wtiproj06.utils.merge_data_frames(1)
        cols_names_modified = [genre.replace("-", "_") for genre in cols_names]
        if table_name == USER_TABLE:
            cols_names_modified.insert(0, "rating")
            cols_names_modified.insert(0, "movie_id")
            cols_names_modified.insert(0, "user_id")
            cols_names_types = ["int", "int", "float"]
            for _ in range(len(cols_names)):
                cols_names_types.append("int")
        else:
            cols_names_modified.insert(0, "user_id")
            cols_names_types = ["int"]
            for _ in range(len(cols_names)):
                cols_names_types.append("float")
        return dict(zip(cols_names_modified, cols_names_types))

    def create_avg_genre_ratings_table(self):
        q = self.create_table(KEYSPACE, AVG_RATINGS, "user_id", self.set_key_and_type(AVG_RATINGS))
        self.session.execute(q)

    def create_user_rated_movies_table(self):
        q = self.create_table(KEYSPACE, USER_TABLE, "user_id, movie_id", self.set_key_and_type(USER_TABLE))
        self.session.execute(q)

    def clear_table(self, table_name):
        self.session.execute(f"TRUNCATE {KEYSPACE}.{table_name};")

    def get_user_data(self, table_name, id):
        data = self.session.execute(f"SELECT * FROM {KEYSPACE}.{table_name} WHERE user_id={str(id)};")
        self.show_off_rows(data)

    def get_n_elements(self, table_name, n):
        data = self.session.execute(f"SELECT * FROM {KEYSPACE}.{table_name} LIMIT {str(n)};")
        return self._convert_keys(data, table_name)

    def get_all_elements(self, table_name):
        data = self.session.execute(f"SELECT * FROM {KEYSPACE}.{table_name};")
        return self._convert_keys(data, table_name)

    def _convert_keys(self, rows, table_name):
        _, primary_cols_name = wtiproj06.utils.merge_data_frames(1)
        if table_name == USER_TABLE:
            primary_cols_name.insert(0, "rating")
            primary_cols_name.insert(0, "movieID")
            primary_cols_name.insert(0, "userID")
        else:
            primary_cols_name.insert(0, "userID")

        to_return = list()
        for row in rows:
            values = list(row.values())
            if table_name == table_name:
                values.insert(2, values[-1])
                to_return.append(dict(zip(primary_cols_name, values)))
        return to_return

    def push_to_rated_movies(self, data):
        self.session.execute("""INSERT INTO """ + KEYSPACE + """.""" + USER_TABLE + """
        (user_id, movie_id, rating,genre_Action, genre_Adventure, genre_Animation,genre_Children,genre_Comedy, genre_Crime,
        genre_Documentary, genre_Drama, genre_Fantasy,genre_Film_Noir, genre_Horror,genre_IMAX, genre_Musical, genre_Mystery,
        genre_Romance, genre_Sci_Fi, genre_Short, genre_Thriller,genre_War,genre_Western)
        VALUES(%(userID)s, %(movieID)s, %(rating)s, %(genre-Action)s, %(genre-Adventure)s, %(genre-Animation)s, %(genre-Children)s,
        %(genre-Comedy)s, %(genre-Crime)s, %(genre-Documentary)s, %(genre-Drama)s, %(genre-Fantasy)s, %(genre-Film-Noir)s, %(genre-Horror)s,
        %(genre-IMAX)s, %(genre-Musical)s, %(genre-Mystery)s, %(genre-Romance)s, %(genre-Sci-Fi)s, %(genre-Short)s,
        %(genre-Thriller)s, %(genre-War)s, %(genre-Western)s)""",
                             {
                                 'userID': data[0],
                                 'movieID': data[1],
                                 'rating': data[2],
                                 'genre-Action': data[3],
                                 'genre-Adventure': data[4],
                                 'genre-Animation': data[5],
                                 'genre-Children': data[6],
                                 'genre-Comedy': data[7],
                                 'genre-Crime': data[8],
                                 'genre-Documentary': data[9],
                                 'genre-Drama': data[10],
                                 'genre-Fantasy': data[11],
                                 'genre-Film-Noir': data[12],
                                 'genre-Horror': data[13],
                                 'genre-IMAX': data[14],
                                 'genre-Musical': data[15],
                                 'genre-Mystery': data[16],
                                 'genre-Romance': data[17],
                                 'genre-Sci-Fi': data[18],
                                 'genre-Short': data[19],
                                 'genre-Thriller': data[20],
                                 'genre-War': data[21],
                                 'genre-Western': data[22],
                             })

    def push_to_avg_ratings(self, data):
        self.session.execute("""INSERT INTO """ + KEYSPACE + """.""" + AVG_RATINGS + """
                (user_id,genre_Action, genre_Adventure, genre_Animation,genre_Children,genre_Comedy, genre_Crime,
                genre_Documentary, genre_Drama, genre_Fantasy,genre_Film_Noir, genre_Horror,genre_IMAX, genre_Musical, genre_Mystery,
                genre_Romance, genre_Sci_Fi, genre_Short, genre_Thriller,genre_War,genre_Western)
                VALUES(%(userID)s, %(genre-Action)s, %(genre-Adventure)s, %(genre-Animation)s, %(genre-Children)s, 
                %(genre-Comedy)s, %(genre-Crime)s, %(genre-Documentary)s, %(genre-Drama)s, %(genre-Fantasy)s, %(genre-Film-Noir)s, %(genre-Horror)s, 
                %(genre-IMAX)s, %(genre-Musical)s, %(genre-Mystery)s, %(genre-Romance)s, %(genre-Sci-Fi)s, %(genre-Short)s,
                %(genre-Thriller)s, %(genre-War)s, %(genre-Western)s)""", data)

    def edit_avg_rating(self, avg_list, id):
        _, primary_cols_name = wtiproj06.utils.merge_data_frames(1)
        to_return = dict()
        to_return["userID"] = int(id)
        for i, genre in enumerate(primary_cols_name):
            to_return[genre] = avg_list[i]
        return to_return

    def push_to_table(self, data, avg=False):
        if avg:
            self.push_to_avg_ratings(data)
        else:
            self.push_to_rated_movies([value for value in data.values()])

    def show_off_rows(self, rows):
        for row in rows:
            print(row)


if __name__ == '__main__':
    cass = CassClient()
    cass.create_user_rated_movies_table()
    cass.create_avg_genre_ratings_table()

    print("\n CLEAR")
    cass.clear_table(USER_TABLE)

    cass.show_off_rows(cass.get_all_elements(USER_TABLE))

    print("\n CLEAR")
    cass.clear_table(AVG_RATINGS)
    DUMMY_DATA = {"userID": 78, "movieID": 903, "rating": 4.0, "genre-Action": 0, "genre-Adventure": 0,
                  "genre-Animation": 0,
                  "genre-Children": 0, "genre-Comedy": 0, "genre-Crime": 0, "genre-Documentary": 0, "genre-Drama": 1,
                  "genre-Fantasy": 0, "genre-Film-Noir": 0, "genre-Horror": 0, "genre-IMAX": 0, "genre-Musical": 0,
                  "genre-Mystery": 1, "genre-Romance": 1, "genre-Sci-Fi": 0, "genre-Short": 0, "genre-Thriller": 1,
                  "genre-War": 0, "genre-Western": 0}

    # cass.push(DUMMY_DATA, avg=True)
    for i in range(5):
        DUMMY_DATA["userID"] = int(i)
        cass.push_to_table(DUMMY_DATA)

    print("ALL USER TABLE")
    cass.show_off_rows(cass.get_all_elements(USER_TABLE))

    print("\nSINGLE USER TABLE")
    cass.get_user_data(USER_TABLE, 1)

    print("\n AVG RATINGS")
    cass.show_off_rows(cass.get_all_elements(AVG_RATINGS))

    print("\n FEW ELEMS")
    cass.show_off_rows((cass.get_n_elements(USER_TABLE, 3)))

    print("\n CLEAR")
    cass.clear_table(USER_TABLE)

    cass.show_off_rows(cass.get_all_elements(USER_TABLE))

    print("\n CLEAR")
    cass.clear_table(AVG_RATINGS)
#
#     cass.show_off_rows(cass.get_all_elements(AVG_RATINGS))
# {
#                                  'userID': data[0],
#                                  'genre-Action': data[1],
#                                  'genre-Adventure': data[2],
#                                  'genre-Animation': data[3],
#                                  'genre-Children': data[4],
#                                  'genre-Comedy': data[5],
#                                  'genre-Crime': data[6],
#                                  'genre-Documentary': data[7],
#                                  'genre-Drama': data[8],
#                                  'genre-Fantasy': data[9],
#                                  'genre-Film-Noir': data[10],
#                                  'genre-Horror': data[11],
#                                  'genre-IMAX': data[12],
#                                  'genre-Musical': data[13],
#                                  'genre-Mystery': data[14],
#                                  'genre-Romance': data[15],
#                                  'genre-Sci-Fi': data[16],
#                                  'genre-Short': data[17],
#                                  'genre-Thriller': data[18],
#                                  'genre-War': data[19],
#                                  'genre-Western': data[20],
#                              }
#     def push_to_rated_movies(self, data):
#         self.session.execute("""INSERT INTO """ + KEYSPACE + """.""" + USER_TABLE + """
#         (user_id, movie_id, rating,genre_Action, genre_Adventure, genre_Animation,genre_Children,genre_Comedy, genre_Crime,
#         genre_Documentary, genre_Drama, genre_Fantasy,genre_Film_Noir, genre_Horror,genre_IMAX, genre_Musical, genre_Mystery,
#         genre_Romance, genre_Sci_Fi, genre_Short, genre_Thriller,genre_War,genre_Western)
#         VALUES(%(userID)s, %(movieID)s, %(rating)s, %(genre-Action)s, %(genre-Adventure)s, %(genre-Animation)s, %(genre-Children)s,
#         %(genre-Comedy)s, %(genre-Crime)s, %(genre-Documentary)s, %(genre-Drama)s, %(genre-Fantasy)s, %(genre-Film-Noir)s, %(genre-Horror)s,
#         %(genre-IMAX)s, %(genre-Musical)s, %(genre-Mystery)s, %(genre-Romance)s, %(genre-Sci-Fi)s, %(genre-Short)s,
#         %(genre-Thriller)s, %(genre-War)s, %(genre-Western)s)""",
#                              {
#                                  'userID': data[0],
#                                  'movieID': data[1],
#                                  'rating': data[2],
#                                  'genre-Action': data[3],
#                                  'genre-Adventure': data[4],
#                                  'genre-Animation': data[5],
#                                  'genre-Children': data[6],
#                                  'genre-Comedy': data[7],
#                                  'genre-Crime': data[8],
#                                  'genre-Documentary': data[9],
#                                  'genre-Drama': data[10],
#                                  'genre-Fantasy': data[11],
#                                  'genre-Film-Noir': data[12],
#                                  'genre-Horror': data[13],
#                                  'genre-IMAX': data[14],
#                                  'genre-Musical': data[15],
#                                  'genre-Mystery': data[16],
#                                  'genre-Romance': data[17],
#                                  'genre-Sci-Fi': data[18],
#                                  'genre-Short': data[19],
#                                  'genre-Thriller': data[20],
#                                  'genre-War': data[21],
#                                  'genre-Western': data[22],
#                              })
#
#     def push_to_avg_ratings(self, data):
#         self.session.execute("""INSERT INTO """ + KEYSPACE + """.""" + AVG_RATINGS + """
#                 (user_id,genre_Action, genre_Adventure, genre_Animation,genre_Children,genre_Comedy, genre_Crime,
#                 genre_Documentary, genre_Drama, genre_Fantasy,genre_Film_Noir, genre_Horror,genre_IMAX, genre_Musical, genre_Mystery,
#                 genre_Romance, genre_Sci_Fi, genre_Short, genre_Thriller,genre_War,genre_Western)
#                 VALUES(%(userID)s, %(genre-Action)s, %(genre-Adventure)s, %(genre-Animation)s, %(genre-Children)s,
#                 %(genre-Comedy)s, %(genre-Crime)s, %(genre-Documentary)s, %(genre-Drama)s, %(genre-Fantasy)s, %(genre-Film-Noir)s, %(genre-Horror)s,
#                 %(genre-IMAX)s, %(genre-Musical)s, %(genre-Mystery)s, %(genre-Romance)s, %(genre-Sci-Fi)s, %(genre-Short)s,
#                 %(genre-Thriller)s, %(genre-War)s, %(genre-Western)s)""", data)
