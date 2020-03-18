import time


def run_consumer(name, consumer):
    print(name, "starts")
    t_end = time.time() + 10
    while time.time() < t_end:
        if consumer.qsize() != 0:
            print(consumer.get())
            print("Rozmiar kolejki", consumer.qsize(), "\n\n")
        else:
            print("QUEUE IS EMPTY")
        time.sleep(1.0 / 4.0)

    print("Consumer ", name, " has stopped")
