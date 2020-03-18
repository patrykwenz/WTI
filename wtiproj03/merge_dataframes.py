import pandas as pd

df1 = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/user_ratedmovies", delimiter="\t",
                  usecols=["userID", "movieID", "rating"])
df2 = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/movie_genres.txt", delimiter="\t")
pd.set_option('expand_frame_repr', False)

rpt = pd.merge(df1, df2, on='movieID')

print(rpt[rpt['movieID'].isin([75])])
