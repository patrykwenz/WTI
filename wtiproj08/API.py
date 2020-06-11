import pandas as pd
from flask import Flask, request
import json
import hashlib
from joblib import load


def convert_dict_to_list_dict(sample):
    to_return = {}
    for key in sample:
        to_return[key] = [sample[key]]
    return pd.DataFrame(to_return)


def get_sample_features_df():
    return pd.read_csv("/home/patryk/PycharmProjects/WTI/wtiproj08/X_test.csv")


def get_hash_id(sample):
    sample = str(sample)
    temp = hashlib.sha224(sample.encode("utf-8")).hexdigest()
    tempint = int(temp, 16)
    return str(tempint % 10000)


class api_logic:

    def __init__(self):
        self.model = "/home/patryk/PycharmProjects/WTI/wtiproj08/diabetes_clf"
        self.mlp = load(self.model + ".joblib")
        self.predictions = {}

    def add_predictions(self, sample):
        sample_df = convert_dict_to_list_dict(sample)
        return self.mlp.predict_proba(sample_df)[0][0]

    def get_prediction(self, sample):
        sample_hash = get_hash_id(sample)
        if sample_hash in self.predictions:
            pred = self.predictions[sample_hash]
        else:
            pred = self.add_predictions(sample)
            self.predictions[sample_hash] = pred
        return pred, sample_hash

    def load_model(self, filename):
        self.model = "/home/patryk/PycharmProjects/WTI/wtiproj08/" + filename
        self.mlp = load(self.model + ".joblib")
        self.predictions = {}
        return "Model loaded"


app = Flask(__name__)
rest_logic = api_logic()


@app.route('/')
def index():
    return json.dumps({"Title": "WTI PROJECT"}), 200


@app.route('/patient-record', methods=["POST"])
def new_rating():
    sample = request.get_json()
    pred, id = rest_logic.get_prediction(sample)
    return json.dumps({"patient_ID": id}), 201


@app.route('/patient-prediction/<pat_id>', methods=["GET"])
def patient_pred(pat_id):
    pred = rest_logic.predictions[pat_id]
    return json.dumps({"probability-of-diabetes": pred}), 200


@app.route('/model', methods=["PUT"])
def new_model():
    model_data = request.get_json()
    file_name = model_data["new_model_file_name"]
    result = rest_logic.load_model(file_name)
    return json.dumps(result), 200


if __name__ == '__main__':
    app.run(debug=True)
