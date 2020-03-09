from wtiproj01.client import *
import time

import pandas as pd


def run_producer(start, stop, producer):
    dataset = pd.read_csv("/home/patryk/PycharmProjects/WTI/resources/user_ratedmovies", delimiter="\t", skiprows=start,
                          nrows=stop)
    pd.set_option('expand_frame_repr', False)
    print("Producer starts")
    for i in range(0, 1000, 100):
        for j, jdict in enumerate(dataset.to_dict(orient='records')[i:i + 100]):
            producer.put({str(j + start + i): jdict})
            time.sleep(1.0 / 100.0)


# producer = Client(0)
# producer.free_db()
# run_producer(0, 1000, producer)







#
# producer = Client(0)
# print("Producer starts")
#
# producer.free_db()
# for i in range(0, 100):
#     producer.put(json.dumps({str(i): str(chr(i))}))
#     time.sleep(0.01)
