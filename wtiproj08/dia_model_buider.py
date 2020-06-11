import pandas as pd
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn import metrics
from joblib import dump, load
import matplotlib.pyplot as plt
import json
import zmq
import time
import requests


def print_request_and_response_info_for_POST_or_PUT(request):
    print("---------------------------------------------------------------")
    print("request.url: ", request.url)
    print("request.status_code: ", request.status_code)
    print("request.headers: ", request.headers)
    print("request.text: ", request.text)
    print("request.request.body: ", request.request.body)
    print("request.request.headers: ", request.request.headers)
    print("---------------------------------------------------------------")


def print_request_and_response_info_for_GET_or_DELETE(request):
    print("---------------------------------------------------------------")
    print("request.url: ", request.url)
    print("request.status_code: ", request.status_code)
    print("request.headers: ", request.headers)
    print("request.text: ", request.text)
    print("request.request.headers: ", request.request.headers)
    print("---------------------------------------------------------------")


if __name__ == '__main__':

    host_IP = "127.0.0.1"
    API_port_number = 5000

    port = str(5555)
    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    socket.bind("tcp://*:%s" % port)

    while True:
        print("Model builder is waiting for a new job request.")
        msg = socket.recv()
        print("Model builder has received a new job request.")
        msg_dict = json.loads(msg.decode('ascii'))
        print(msg_dict)
        msg_dict_value = msg_dict["job_request_ID"]
        print(msg_dict_value)
        print("Job execution is starting.")

        X = pd.read_csv("X_train.csv")
        y = pd.read_csv("y_train.csv")

        X_train, X_test, y_train, y_test = train_test_split(X, y)

        mlp = MLPClassifier()
        MLPClassifier_parameter_space = {
            'hidden_layer_sizes': [(50, 50, 50), (50, 100, 50), (100,)],
            'activation': ['tanh', 'relu'],
            'solver': ['sgd', 'adam'],
            'alpha': [0.0001, 0.05],
            'learning_rate': ['constant', 'adaptive'],
        }
        dev_MLPClassifier_parameter_space = {
            'hidden_layer_sizes': [(50)],
            'activation': ['tanh'],
            'solver': ['adam'],
            'alpha': [0.05],
            'learning_rate': ['adaptive'],
        }
        MLPClassifier_parameter_space = dev_MLPClassifier_parameter_space
        clf = GridSearchCV(mlp, MLPClassifier_parameter_space, n_jobs=1, cv=3)
        clf.fit(X_train, y_train)
        best_params = clf.best_params_
        clf = MLPClassifier(**best_params)
        clf.fit(X, y)
        model_name = "diabetes_clf"
        dump(clf, model_name + '.joblib')
        print("Model is completed.")
        print("Model executor is about to report the completion of the job back to the requester.")
        msg_content_dict = {}
        msg_content_dict["completed_job_ID"] = msg_dict_value
        msg_content_dict["new_model_file_name"] = model_name
        msg_content = str(json.dumps(msg_content_dict))
        msg_string = msg_content.encode('ascii')
        socket.send(msg_string)

