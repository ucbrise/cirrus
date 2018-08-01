import random


# Generates a random RGB color
def get_random_color():
    def rand_256(): return random.randint(0, 255)
    return 'rgb(%d, %d, %d)' % (rand_256(), rand_256(), rand_256())


# Takes a dictionary in the form of { 'machine-public-ip': ['list of commands'] }
# and creates a bash file for each machine that will run the command list
def command_dict_to_file(command_dict):
    for key, no in zip(command_dict.keys(), range(len(command_dict.keys()))):
        lst = command_dict[key]

        with open("machine_%d.sh" % no, "w") as f:
            for cmd in lst:
                f.write(cmd + "\n\n")
