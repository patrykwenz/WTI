import pandas as pd
import numpy as np
import warnings


def warn(*args, **kwargs):
    pass


warnings.warn = warn


def merge_data_frames(nrows):
    df1 = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/user_ratedmovies", delimiter="\t",
                      usecols=["userID", "movieID", "rating"], nrows=nrows)
    df2 = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/movie_genres.txt", delimiter="\t")
    pd.set_option('expand_frame_repr', False)
    df2["valuetofill"] = 1
    df2 = df2.pivot_table(index="movieID", columns="genre", values="valuetofill", fill_value=0)

    genres_to_return = []
    for column_name in df2.columns:
        new_genre_column_name = "genre-" + column_name
        df2.rename(columns={column_name: new_genre_column_name}, inplace=True)
        genres_to_return.append(new_genre_column_name)
    merged_table = pd.merge(df1, df2, on='movieID')
    return merged_table, genres_to_return


def get_json_rows(merged_table):
    to_return = list()
    for jdict in merged_table.to_dict(orient='records'):
        to_return.append(jdict)
    return to_return


def convert_list_of_dcits_to_dataframe(list_of_dicts):
    return pd.DataFrame(list_of_dicts)


# print(convert_list_of_dcits_to_dataframe(get_json_rows(merge_data_frames(5)[0])))
def get_avg_genre_rating(merged_table, genres_cols, difference=True):
    ratings = merged_table["rating"].values
    ratings_reshaped = ratings.reshape(-1, 1)
    movie_genres_cols_values = merged_table[genres_cols].values

    other_cols_values = merged_table.drop(genres_cols, axis=1)
    rating_to_genre = ratings_reshaped * movie_genres_cols_values

    rating_to_genre[rating_to_genre == 0] = np.nan
    avg_genres_ratings = np.nanmean(rating_to_genre, axis=0)

    avg_genres_ratings[np.isnan(avg_genres_ratings)] = 0
    rating_to_genre[np.isnan(rating_to_genre)] = 0

    if difference:
        avg_matrix = avg_genres_ratings.reshape(1, -1)
        final_ratings = rating_to_genre - avg_matrix
    else:
        final_ratings = rating_to_genre

    other_and_final_merged = np.concatenate((other_cols_values, final_ratings), axis=-1)
    df_to_return = pd.DataFrame(other_and_final_merged, columns=merged_table.columns)
    return df_to_return, avg_genres_ratings


def get_single_user_ratings(merged_table, genres_cols, id):
    non_diff_ratings, avg_genres_ratings = get_avg_genre_rating(merged_table, genres_cols, difference=False)
    user_ratings = non_diff_ratings.where(non_diff_ratings["userID"] == id)

    user_ratings[user_ratings == 0] = np.NaN
    avg_single_user_ratings = np.nanmean(user_ratings[genres_cols].values, axis=0)
    avg_single_user_ratings[np.isnan(avg_single_user_ratings)] = 0

    return avg_single_user_ratings


def get_user_profile(merged_table, genres_cols, id):
    avg_single_user_ratings = get_single_user_ratings(merged_table, genres_cols, id)
    _, avg_ratings = get_avg_genre_rating(merged_table, genres_cols, difference=False)
    user_profile = avg_single_user_ratings - avg_ratings

    return user_profile
