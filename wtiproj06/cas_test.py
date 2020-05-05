from cassandra.cluster import Cluster
from cassandra.query import dict_factory


def create_keyspace(session, keyspace):
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS """ + keyspace + """
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
    """)


def create_table(session, keyspace, table):
    session.execute("""
    CREATE TABLE IF NOT EXISTS """ + keyspace + """.""" + table + """ (
    user_id int ,
    avg_movie_rating float,
    PRIMARY KEY(user_id)
    )
    """)


def push_data_table(session, keyspace, table, userId, avgMovieRating):
    session.execute(
        """
        INSERT INTO """ + keyspace + """.""" + table + """ (user_id, avg_movie_rating)
    VALUES (%(user_id)s, %(avg_movie_rating)s)
    """,
        {
            'user_id': userId,
            'avg_movie_rating': avgMovieRating
        }
    )


def get_data_table(session, keyspace, table):
    rows = session.execute("SELECT * FROM " + keyspace + "." + table + ";")
    for row in rows:
        print("Table:",row)


def clear_table(session, keyspace, table):
    session.execute("TRUNCATE " + keyspace + "." + table + ";")


def delete_table(session, keyspace, table):
    session.execute("DROP TABLE " + keyspace + "." + table + ";")


if __name__ == "__main__":
    keyspace = "user_ratings"
    table = "user_avg_rating"
    # utworzenia połączenia z klastrem
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()
    # utworzenie nowego keyspace
    print("New keyspace created")
    create_keyspace(session, keyspace)
    # ustawienie używanego keyspace w sesji
    print("Keyspace set")
    session.set_keyspace(keyspace)
    # użycie dict_factory pozwala na zwracanie słowników
    # znanych z języka Python przy zapytaniach do bazy danych
    session.row_factory = dict_factory
    print("New table created")
    # tworzenie tabeli
    create_table(session, keyspace, table)
    clear_table(session, keyspace, table)
    print("Table:", get_data_table(session, keyspace, table))
    # umieszczanie danych w tabeli
    print("Data added")
    push_data_table(session, keyspace, table, userId=1337, avgMovieRating=4.2)
    get_data_table(session, keyspace, table)
    # czyszczenie zawartości tabeli
    print("Table cleared")
    clear_table(session, keyspace, table)
    print("Table:", get_data_table(session, keyspace, table))
    # usuwanie tabeli
    delete_table(session, keyspace, table)
