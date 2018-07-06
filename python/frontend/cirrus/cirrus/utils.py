

import random


def get_random_color():
    r = lambda: random.randint(0,255)
    return 'rgb(%d, %d, %d)' % (r(),r(),r())
