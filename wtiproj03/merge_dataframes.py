import pandas as pd
import numpy as np
import json

df1 = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/user_ratedmovies", delimiter="\t",
                  usecols=["userID", "movieID", "rating"])
df2 = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/movie_genres.txt", delimiter="\t")
pd.set_option('expand_frame_repr', False)

t = pd.merge(df1, df2, on='movieID')


# print(t[t['userID'].isin([75])])

#
# filterinfDataframe = t[(t['userID'] == 75) & (t['movieID'] == 45722)]
# print(filterinfDataframe)
df_new = df2.groupby('movieID')['genre'].apply(list).reset_index(name='genre')

rpt = pd.merge(df1, df_new, on='movieID')
print(rpt)

# filterinfDataframe = rpt[(rpt['userID'] == 75) & (rpt['movieID'] == 45722)]
# print(filterinfDataframe)


# dummy_frame_keys = ["userID", "movieID", "rating"] + list("genre-" + rpt['genre'].unique())

def get_genres_names():
    return list(t['genre'].unique())


def generate_json_data():
    genres = get_genres_names()
    l = []
    for jdict in rpt.to_dict(orient='records')[0:10]:
        list_of_genres = jdict["genre"]
        del jdict["genre"]
        for key in genres:
            temp = "genre-" + key
            dummy_val = 0
            if key in list_of_genres:
                dummy_val = 1
            jdict[temp] = dummy_val
        # print(jdict)
        l.append(jdict)
    return l
