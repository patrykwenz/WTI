class Queue(object):
    def __init__(self):
        self.container = list()

    def add_item(self, item):
        self.container.insert(0, item)

    def get_item(self):
        return self.container.pop()

    def is_empty(self):
        return self.container == []

    def size(self):
        return len(self.container)

    def empty_all(self):
        while not self.is_empty():
            print(self.get_item())

    def add_items(self, items):
        for item in items:
            self.add_item(item)


if __name__ == '__main__':
    q = Queue()
    q.add_items([1, 2, 3, 4, 5, 6])
    q.empty_all()
