import pandas as pd

df1 = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/user_ratedmovies", delimiter="\t",
                  usecols=["userID", "movieID", "rating"])
df2 = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/movie_genres.txt", delimiter="\t")

pd.set_option('expand_frame_repr', False)

df2["valuetofill"] = 1
df2 = df2.pivot_table(index="movieID", columns="genre", values="valuetofill", fill_value=0)

for column_name in df2.columns:
    df2.rename(columns={column_name: "genre-" + column_name}, inplace=True)

merged_table = pd.merge(df1, df2, on='movieID')


def get_json_rows():
    to_return = list()
    for jdict in merged_table.to_dict(orient='records')[0:150]:
        to_return.append(jdict)
    return to_return


def get_labels_names():
    return list(df2.columns)
