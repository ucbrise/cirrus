

import utils


class cirrus_bundle:

    # Graph interfacer

    def __init__(self):
        self.cirrus_objs = []
        self.infos = []
        self.param_lst = []
        self.num_jobs = 1
        pass

    def set_task_parameters(task, param_dict_lst):
        self.param_lst = param_dict_lst

        for param in param_dict_lst:
            c = task(**param)
            self.cirrus_obj.append(c)

    def set_jobs(n):
        self.num_jobs = n

    def set_existing_machines():
        pass

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
        for cirrus_obj in self.cirrus_objs:
            loss = cirrus_obj.get_last_loss()
            lst.append((index, loss))
            index += 1
        lst.sort()
        top = lst[:n]
        return [cirrus_obj.get_time_loss() for cirrus_obj in top]

    def kill(i):
        self.cirrus_objs[i].kill()
