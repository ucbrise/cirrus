import time
import random
import app

from cirrus import *

class mock:


    def __init__(self, name):
        self.name = name
        self.time_loss = []
        self.start_time = time.time()
        self.counter = 0
        self.dead = False


    def get_name(self):
        return self.name

    def get_time_loss(self):
        if self.counter % 2 == 0 and not self.dead:
            t = time.time() - self.start_time
            loss = random.random()
            self.time_loss.append((t, loss))
        self.counter += 1
        return self.time_loss

    def kill(self):
        self.dead = True


if __name__ == "__main__":
    m1 = mock("Mock 0")
    m2 = mock("Mock 1")
    m3 = mock("Mock 2")
    m4 = mock("Mock 3")

    lst = [m1, m2, m3, m4]
    app.bundle = lst
    app.app.run_server(debug=False)
