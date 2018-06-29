

import utils


class cirrus_bundle:

    # Graph interfacer

    def __init__(self):
        self.cirrus_objs = []
        self.infos = []
        pass

    def set_parameters(param):
        pass

    def set_jobs(n):
        pass

    def get_num_experiments(self):
        return len(cirrus_objs)

    def add_experiments(lst):
        self.cirrus_objs.extend(lst)
        graph_info = {}

        # Assign a uniue color to each graph
        graph_info['color'] = get_random_color()

        self.infos.append(graph_info)

    # Get data regarding experiment i.
    def get_info(i, param=None):
        out = self.infos[i]
        if not param:
            return out[param]
        else:
            return out

    # Gets the top n. If n == 0, gets all. Else gets last n
    def get_top(n):
        index = 0
        lst = []
        for cirrus_obj in cirrus_objs:
            loss = cirrus_obj.get_last_loss()
            lst.append((index, loss))
            index += 1
        lst.sort()
        top = lst[:n]
        return [cirrus_obj.get_time_loss() for cirrus_obj in top]

    def kill(i):
        cirrus_objs[i].kill()
