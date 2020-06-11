import pandas as pd
import requests
from sklearn.model_selection import train_test_split
from sklearn import metrics
import matplotlib.pyplot as plt
# import zmq
import time
import json


def get_test_samples_features_df():
    test_df = pd.read_csv("X_test.csv")
    return test_df


def get_test_samples_labels_df():
    test_true_labels_df = pd.read_csv("y_test.csv")
    return test_true_labels_df


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


X_test = pd.read_csv("X_test.csv")
y_test = pd.read_csv("y_test.csv")
host_IP = "127.0.0.1"
API_port_number = 5000
model_builder_job_request_ID = str(hash(time.time()))
model_name = "diabetes_clf"
received_new_model_metadata_dict = {}
received_new_model_metadata_dict["completed_job_ID"] = model_builder_job_request_ID
received_new_model_metadata_dict["new_model_file_name"] = model_name
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
