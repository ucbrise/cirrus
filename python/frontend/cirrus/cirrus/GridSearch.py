import itertools
import logging
import os
import threading
import time

from utils import *

logging.basicConfig(filename="cirrusbundle.log", level=logging.WARNING)

class GridSearch:

    # Graph interfacer


    # TODO: Add some sort of optional argument checking
    def __init__(self, task=None, param_base=None, hyper_vars=[], hyper_params=[], machines=[], num_jobs=1, timeout=-1):

        # Private Variables
        self.cirrus_objs = [] # Stores each singular experiment
        self.infos = []       # Stores metadata associated with each experiment
        self.param_lst = []   # Stores parameters of each experiment
        self.check_queue = [] # Queue for checking error/lambdas for each object
        self.threads = []
        self.kill_signal = threading.Event()
        self.loss_lst = []
        self.start_time = time.time()

        # User inputs
        self.set_timeout = timeout # Timeout. -1 means never timeout
        self.num_jobs = num_jobs     # Number of threads checking check_queue
        self.hyper_vars = hyper_vars
        self.machines = machines

        # Setup
        self.set_task_parameters(
                task,
                param_base=param_base,
                hyper_vars=hyper_vars,
                hyper_params=hyper_params,
                machines=machines)


    # TODO: Add some sort of optional argument checking
    # User must either specify param_dict_lst, or hyper_vars, hyper_params, and param_base
    def set_task_parameters(self, task, param_base=None, hyper_vars=[], hyper_params=[], machines=[]):
        possibilities = list(itertools.product(*hyper_params))
        base_port = 1337
        print(possibilities)
        for p in possibilities:
            configuration = zip(hyper_vars, p)
            print(configuration)
            modified_config = param_base.copy()
            for var_name, var_value in configuration:
                modified_config[var_name] = var_value
            modified_config['ps_ip_port'] = base_port
            base_port += 2
            c = task(**modified_config)
            self.cirrus_objs.append(c)
            self.infos.append({'color': get_random_color()})
            self.loss_lst.append({})
            self.param_lst.append(modified_config)

    def get_info_for(self, i):
        string = ""
        for param_name in self.hyper_vars:
            string += "%s: %s\n" % (param_name, str(self.param_lst[i][param_name]))
        return string;

    def get_name_for(self, i):
        out = self.cirrus_objs[i].get_name()
        return out

    def get_cost(self):
        cost = 0
        for i in range(len(self.param_lst)):
            c = self.cirrus_objs[i]
            d = self.param_lst[i]
            cost += c.cost_model.get_cost(time.time() - self.start_time)
        return cost

    def get_cost_per_sec(self):
        return sum([c.cost_model.get_cost_per_second() for c in self.cirrus_objs])

    def get_num_lambdas(self):
        return sum([c.get_num_lambdas(fetch=False) for c in self.cirrus_objs])

    def get_xs_for(self, i, metric="LOSS"):
        if metric == "LOSS":
            lst = self.loss_lst[i]
        if metric == "UPS":
            lst = self.cirrus_objs[i].get_updates_per_second(fetch=False)
        return [item[0] for item in lst]

    def get_total_command(self):
        cmd_lst = []
        for c in self.cirrus_objs:
            cmd_lst.append(c.get_command())
        return ' '.join(cmd_lst)

    def get_ys_for(self, i, metric="LOSS"):
        if metric == "LOSS":
            lst = self.loss_lst[i]
        if metric == "UPS":
            lst = self.cirrus_objs[i].get_updates_per_second(fetch=False)
        return [item[1] for item in lst]

    def start_queue_threads(self):
        def custodian(cirrus_objs, thread_id, num_jobs, infos):
            index = thread_id
            logging.info("Custodian number %d starting..." % thread_id)
            seen = []
            start_time = time.time()

            while True:
                cirrus_obj = cirrus_objs[index]

                cirrus_objs[index].relaunch_lambdas()
                loss = cirrus_objs[index].get_time_loss()
                print("Thread", thread_id, "Machine", index, "Loss", loss)
                self.loss_lst[index] = loss
                index += num_jobs
                if index >= len(cirrus_objs):
                    index = thread_id

                    # Dampener to prevent too many calls at once
                    if time.time() - start_time < 1:
                        time.sleep(1)
                    start_time = time.time()

            logging.info("Thread number %d is exiting" % thread_id)

        # Dictionary of commands per machine
        command_dict = {}
        for machine in self.machines:
            command_dict[machine] = []

        for c in self.cirrus_objs:
            c.get_command(command_dict)

        command_dict_to_file(command_dict)

        copy_threads = min(len(self.machines), self.num_jobs)

        def copy_and_run(thread_id):
            while True:
                if thread_id >= len(self.machines):
                    return
                cmd = 'ssh ubuntu@%s "killall parameter_server"' % (self.machines[thread_id])
                print cmd
                os.system(cmd)
                cmd = "scp machine_%d.sh ubuntu@%s:~/" % (thread_id, self.machines[thread_id])
                print cmd
                os.system(cmd)
                cmd = 'ssh ubuntu@%s "chmod +x machine_%d.sh &"' % (self.machines[thread_id], thread_id)
                print cmd
                os.system(cmd)
                cmd = 'ssh ubuntu@%s "./machine_%d.sh &"' % (self.machines[thread_id], thread_id)
                print cmd
                os.system(cmd)
                thread_id += copy_threads
                print("Done")

        p_lst = []
        for i in range(copy_threads):
            p = threading.Thread(target=copy_and_run, args=(i,))
            p.start()
            p_lst.append(p)

        [p.join for p in p_lst]

        for i in range(self.num_jobs):
            p = threading.Thread(target=custodian, args=(self.cirrus_objs, i, self.num_jobs, self.infos))
            p.start()

    def get_number_experiments(self):
        return len(self.cirrus_objs)

    def set_jobs(self, n):
        self.num_jobs = n

    def set_existing_machines(self):
        pass

    def run(self, index):
        assert(0 <= index < len(self.cirrus_objs))
        cirrus_obj = self.cirrus_objs[index]
        self.infos[index]['start_time'] = time.time()
        cirrus_obj.run()


    def run_bundle(self):
        self.start_queue_threads()

    def kill_all(self):
        for cirrus_ob in self.cirrus_objs:
            cirrus_ob.kill()

    # Get data regarding experiment i.
    def get_info(self, i, param=None):
        out = self.infos[i]
        if param:
            return out[param]
        else:
            return out

    # Gets the top n. If n == 0, gets all. Else gets last n
    def get_top(self, n):
        index = 0
        lst = []
        for cirrus_obj in self.cirrus_objs:
            loss = cirrus_obj.get_time_loss()
            if (len(loss) == 0):
                continue
            lst.append((index, loss[-1]))
            index += 1
        lst.sort(key = lambda x: x[1][1])
        if n < 0:
            top = lst[n:]
        else:
            top = lst[:n]
        return top

    def kill(self, i):
        self.cirrus_objs[i].kill()
