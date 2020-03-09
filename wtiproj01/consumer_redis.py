import time

from wtiproj01.client import *


#
# while not consumer.is_empty():
#     print(consumer.get())
#     time.sleep(2)
#     print("dostepna lista", consumer.qsize(), "\n\n")

def run_consumer(name, consumer):
    print(name, "starts")
    t_end = time.time() + 10
    while time.time() < t_end:
        print(consumer.get())
        print("Rozmiar kolejki", consumer.qsize(), "\n\n")
        time.sleep(1.0 / 4.0)

# consumer = Client(0)
# run_consumer("#1Consumer", consumer)
