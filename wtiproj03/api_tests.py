import requests
import time


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


def test():
    IP = "http://127.0.0.1:5000/"
    DUMMY_ID = "78"
    DUMMY_DATA = {"userID": 78, "movieID": 903, "rating": 4.0, "genre-Action": 0, "genre-Adventure": 0,
                  "genre-Animation": 0,
                  "genre-Children": 0, "genre-Comedy": 0, "genre-Crime": 0, "genre-Documentary": 0, "genre-Drama": 1,
                  "genre-Fantasy": 0, "genre-Film-Noir": 0, "genre-Horror": 0, "genre-IMAX": 0, "genre-Musical": 0,
                  "genre-Mystery": 1, "genre-Romance": 1, "genre-Sci-Fi": 0, "genre-Short": 0, "genre-Thriller": 1,
                  "genre-War":
                      0, "genre-Western": 0}

    # POST /RATING
    request = requests.post(IP + "rating", json=DUMMY_DATA)
    POST_info(request)

    # GET /RATINGS
    request = requests.get(IP + "ratings")
    GET_DELETE_info(request, "GET")

    # GET /avg-genre-ratings/all-users
    request = requests.get(IP + "avg-genre-ratings/all-users")
    GET_DELETE_info(request, "GET")

    # GET /avg-genre-ratings/all-users
    request = requests.get(IP + "avg-genre-ratings/" + "75")
    GET_DELETE_info(request, "GET")

    # DELETE /RATINGS
    request = requests.delete(IP + "ratings")
    GET_DELETE_info(request, "DELETE")


import multiprocessing
import time


def TEST_GET_ON_N_THREADS(n):
    proc = []
    s = time.time()
    for _ in range(n):
        p = multiprocessing.Process(target=test)
        p.start()
        proc.append(p)

    for p in proc:
        p.join()

    end = time.time() - s
    return end


if __name__ == '__main__':

    test()
    # tab = []
    # for i in range(100, 1001, 100):
    #     s = time.time()
    #     for _ in range(0, i):
    #         test()
    #     end = time.time() - s
    #     tab.append(end)
    #
    # print(tab)
    # with open("t.txt", "w") as file:
    #     for t in tab:
    #         file.write(str(t) + "\n")
