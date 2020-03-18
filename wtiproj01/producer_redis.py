import time

import pandas as pd


def run_producer(start, stop, producer):
    dataset = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/user_ratedmovies", delimiter="\t", skiprows=start,
                          nrows=stop)
    pd.set_option('expand_frame_repr', False)
    print("Producer starts")
    for j, jdict in enumerate(dataset.to_dict(orient='records')):
        producer.put({str(j + start): jdict})
        time.sleep(2)
