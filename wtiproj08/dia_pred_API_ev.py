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


diabetes = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/diabetes.csv")
X = diabetes.loc[:, diabetes.columns != 'Outcome']
y = diabetes['Outcome']
X_train, X_test, y_train, y_test = train_test_split(X, y)
X_train.to_csv("X_train.csv", index=False)
y_train.to_csv("y_train.csv", index=False)

host_IP = "127.0.0.1"
API_port_number = 5000

port = str(5555)
context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.connect("tcp://localhost:%s" % port)

job_ID = str(hash(time.time()))
msg_content_dict = {}
msg_content_dict["job_request_ID"] = job_ID
msg_content = str(json.dumps(msg_content_dict))
msg_string = msg_content.encode('ascii')
print("A new job request is about to be sent to the job executor.")
socket.send(msg_string)
print("Job requester is waiting for confirmation of the new job completion.")
msg = socket.recv()
received_new_model_metadata_dict = json.loads(msg.decode('ascii'))
received_new_model_metadata_dict_value = received_new_model_metadata_dict[list(received_new_model_metadata_dict)[0]]
if received_new_model_metadata_dict_value == job_ID:
    print("Job requester has received the confirmation of the new job completion.")
else:
    print("Job requester has received some unexpected message.")

request = requests.put("http://" + host_IP + ":" + str(API_port_number) + "/model",
                       json=received_new_model_metadata_dict)
model_loading_result_dict = request.text
print_request_and_response_info_for_POST_or_PUT(request)

predictions = []
row_iterator = X_test.iterrows()
for row in row_iterator:
    row_as_dict = row[1].to_dict()
    request = requests.post("http://" + host_IP + ":" + str(API_port_number) + "/patient-record",
                            json=row_as_dict)
    patient_ID_dict = request.json()
    print_request_and_response_info_for_POST_or_PUT(request)
    patient_ID = patient_ID_dict["patient_ID"]
    request = requests.get("http://" + host_IP + ":" + str(API_port_number) + "/patient-prediction/" + patient_ID)
    prediction_dict = request.json()
    prediction = prediction_dict["probability-of-diabetes"]
    print("prediction: ", prediction)
    predictions.append(prediction)
    print_request_and_response_info_for_GET_or_DELETE(request)

fpr, tpr, thresholds = metrics.roc_curve(y_test, predictions, pos_label=0)
AuROC = metrics.auc(fpr, tpr)

plt.figure()
plt.plot(fpr, tpr, color='darkorange', lw=2, label='ROC curve (area = %0.2f)' % AuROC)
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('Receiver Operating Characteristic')
plt.legend(loc="lower right")
plt.show()
