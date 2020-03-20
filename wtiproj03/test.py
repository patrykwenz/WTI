import requests

api_url = 'http://127.0.0.1:5000/'


def check_rating(endpoint="rating", url=api_url):
    url = url + endpoint
    resp = requests.post(url, {"userID": 78, "movieID": 903, "rating": 4.0, "genre-Action": 0, "genre-Adventure": 0,
                               "genre-Animation": 0, "genre-Children": 0,
                               "genre-Comedy": 0, "genre-Crime": 0, "genre-Documentary": 0, "genre-Drama": 1,
                               "genre-Fantasy": 0, "genre-Film-Noir": 0, "genre-Horror": 0,
                               "genre-IMAX": 0, "genre-Musical": 0, "genre-Mystery": 1, "genre-Romance": 1,
                               "genre-Sci-Fi": 0, "genre-Short": 0, "genre-Thriller": 1, "genre-War":
                                   0, "genre-Western": 0})
    assert resp.status_code == 201
    print(resp.headers)
    print(resp.text)


def check_ratings(endpoint="ratings", url=api_url):
    url = url + endpoint
    resp = requests.get(url)
    assert resp.status_code == 200
    print(resp.headers)
    print(resp.text)


check_rating()
check_ratings()
