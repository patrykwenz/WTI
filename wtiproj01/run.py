import os
from multiprocessing import Pool, Process
from wtiproj01.consumer_redis import run_consumer, Client
from wtiproj01.producer_redis import run_producer

# processes_cons = ('producer1.py', 'consumer2.py', 'consumer1.py',)
# processes_prod = ('producer1.py', 'consumer1.py', 'producer2.py')
#
#
# def run_process(process):
#     os.system('python {}'.format(process))
#
#
# def run_with_2_cons():
#     pool = Pool(processes=3)
#     pool.map(run_process, processes_cons)
#
#
# def run_with_2_prods():
#     pool = Pool(processes=3)
#     pool.map(run_process, processes_prod)
#
#
# if __name__ == '__main__':
#     run_with_2_prods()


prod1 = Client(0)
prod2 = Client(0)
cons2 = Client(0)
cons1 = Client(0)


def parralel_2_producers():
    p1 = Process(target=run_producer, args=(0, 1000, prod1))
    p2 = Process(target=run_producer, args=(2000, 3000, prod2))
    c1 = Process(target=run_consumer, args=("#1 Consumer", cons1))

    prod1.free_db()

    p1.start()
    p2.start()
    c1.start()
    p1.join()
    p2.join()


def parallel_2_consumers():
    p1 = Process(target=run_producer, args=(0, 1000, prod1))
    c1 = Process(target=run_consumer, args=("#1 Consumer", cons1))
    c2 = Process(target=run_consumer, args=("#2 Consumer", cons2))

    prod1.free_db()

    p1.start()
    c1.start()
    c2.start()
    p1.join()
    c1.join()
    c2.join()


def test():
    p1 = Process(target=run_producer, args=(0, 1000, prod1))
    c1 = Process(target=run_consumer, args=("#1 Consumer", cons1))

    prod1.free_db()

    p1.start()
    c1.start()
    p1.join()
    c1.join()

if __name__ == '__main__':
    parallel_2_consumers()