

import random

def get_random_color():
    r = lambda: random.randint(0,255)
    return 'rgb(%d, %d, %d)' % (r(),r(),r())


def command_dict_to_file(command_dict):
    for key, no in zip(command_dict.keys(), range(len(command_dict.keys()))):
        lst = command_dict[key]

        with open("machine_%d.sh" % no, "w") as f:
            for cmd in lst:
                f.write(cmd + "\n\n")
