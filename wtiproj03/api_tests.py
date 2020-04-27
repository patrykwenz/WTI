import requests
import time
from wtiproj03.merge_utils import get_json_rows

def GET_DELETE_info(request, info):
    print(f"########  {info} ########")
    print(f"request.url: {request.url}")
    print(f"request.status_code: {request.status_code}")
    print(f"request.headers: {request.headers}")
    print(f"request.text: {request.text}")
    print(f"request.request.headers: {request.request.headers}")
    print("################################\n")


def POST_info(request):
    print("########  POST ########")

    print(f"request.url: {request.url}")
    print(f"request.status_code: {request.status_code}")
    print(f"request.headers: {request.headers}")
    print(f"request.text: {request.text}")
    print(f"request.request.body: {request.request.body}")
    print(f"request.request.headers: {request.request.headers}")

    print("################################\n")

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
def test():
    IP = "http://127.0.0.1:5000/"
    IP2 ="http://127.0.0.1:9898/"

    SCORES = dict()
    SCORES["flask"] = list()
    SCORES["cherry"] = list()

    rows = get_json_rows()
    for row in rows:
        request = requests.post(IP + "rating", json=row)
        SCORES["flask"].append(request.elapsed.total_seconds())

    for row in rows:
        request = requests.post(IP2 + "rating", json=row)
        SCORES["cherry"].append(request.elapsed.total_seconds())


    plt.plot(SCORES["flask"],color=(0,1,0), label="flask")
    plt.plot(SCORES["cherry"],color=(1,0,0), label="cherry")

    red_patch = mpatches.Patch(color='red', label='cherrypy')
    green_patch = mpatches.Patch(color='green', label='flask')
    plt.legend(handles=[red_patch, green_patch])
    plt.show()

    # GET /RATINGS
    # request = requests.get(IP + "ratings")
    # GET_DELETE_info(request, "GET")

# POST /RATING
    # request = requests.post(IP + "rating", json=DUMMY_DATA)
    # POST_info(request)
    #
    # # GET /RATINGS
    # request = requests.get(IP + "ratings")
    # GET_DELETE_info(request, "GET")
    #
    # # GET /avg-genre-ratings/all-users
    # request = requests.get(IP + "avg-genre-ratings/all-users")
    # GET_DELETE_info(request, "GET")
    #
    # # GET /avg-genre-ratings/all-users
    # request = requests.get(IP + "avg-genre-ratings/" + "75")
    # GET_DELETE_info(request, "GET")
    #
    # # DELETE /RATINGS
    # request = requests.delete(IP + "ratings")
    # GET_DELETE_info(request, "DELETE")


# DUMMY_ID = "78"
#     DUMMY_DATA = {"userID": 78, "movieID": 903, "rating": 4.0, "genre-Action": 0, "genre-Adventure": 0,
#                   "genre-Animation": 0,
#                   "genre-Children": 0, "genre-Comedy": 0, "genre-Crime": 0, "genre-Documentary": 0, "genre-Drama": 1,
#                   "genre-Fantasy": 0, "genre-Film-Noir": 0, "genre-Horror": 0, "genre-IMAX": 0, "genre-Musical": 0,
#                   "genre-Mystery": 1, "genre-Romance": 1, "genre-Sci-Fi": 0, "genre-Short": 0, "genre-Thriller": 1,
#                   "genre-War":
#                       0, "genre-Western": 0}
if __name__ == '__main__':

    test()
