import redis
import json


class Client(object):
    def __init__(self, db, namespace="queue", name="test", host="localhost", charset="utf8", port=6381):
        self.client = redis.StrictRedis(host=host, charset=charset, port=port, db=db)
        self.q_key = '%s:%s' % (namespace, name)

    def qsize(self):
        return self.client.llen(self.q_key)

    def is_empty(self):
        return self.qsize() == 0

    def free_db(self):
        self.client.flushdb()

    def put(self, item):
        self.client.rpush(self.q_key, json.dumps(item))

    def cut_list(self, start, stop):
        self.client.ltrim(self.q_key, start, stop)

    def send_messages(self, msgs):
        for msg in msgs:
            self.put(json.dumps(msg))

    def get(self, block=True, timeout=None):
        if block:
            item = self.client.blpop(self.q_key, timeout=timeout)
        else:
            item = self.client.lpop(self.q_key)

        if item:
            item = item[1]
        return item

    def show_queue(self):
        while self.qsize() != 0:
            print(self.client.lpop(self.q_key))

    def get_items(self, start, stop):
        return self.client.lrange(self.q_key, 0, -1)

#
# if __name__ == '__main__':
#     producer = Client(0)
#     for i in range(42, 100):
#         producer.put(json.dumps({str(i): str(chr(i))}))
#     # producer.cut_list(0, 10)
#
#     # producer.show_queue()
#     items = producer.get_items(0,-1)
#     print(items)
