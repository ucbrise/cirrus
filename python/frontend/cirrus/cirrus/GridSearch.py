import itertools
import logging
import os
import threading
import time

import graph
from utils import *
import automate
import setup

logging.basicConfig(filename="cirrusbundle.log", level=logging.WARNING)

class GridSearch:


    # TODO: Add some sort of optional argument checking
    def __init__(self, task=None, param_base=None, hyper_vars=[], hyper_params=[], instances=[], num_jobs=1, timeout=-1):

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
        self.instances = instances

        # Setup
        self.set_task_parameters(
                task,
                param_base=param_base,
                hyper_vars=hyper_vars,
                hyper_params=hyper_params,
                instances=instances)

        self.adjust_num_threads()

    def adjust_num_threads(self):
        # make sure we don't have more threads than experiments
        self.num_jobs = min(self.num_jobs, len(self.cirrus_objs))


    # User must either specify param_dict_lst, or hyper_vars, hyper_params, and param_base
    def set_task_parameters(self, task, param_base=None, hyper_vars=[], hyper_params=[], instances=[]):
        possibilities = list(itertools.product(*hyper_params))
        base_port = 1337
        index = 0
        num_machines = len(instances)
        for p in possibilities:
            configuration = zip(hyper_vars, p)
            modified_config = param_base.copy()
            for var_name, var_value in configuration:
                modified_config[var_name] = var_value
            modified_config["ps"] = automate.ParameterServer(instances[index], base_port, base_port+1,
                                                             modified_config["n_workers"] * 2)
            index = (index + 1) % num_machines
            base_port += 2

            c = task(**modified_config)
            self.cirrus_objs.append(c)
            self.infos.append({'color': get_random_color()})
            self.loss_lst.append({})
            self.param_lst.append(modified_config)

    # Fetches custom metadata from experiment i
    def get_info_for(self, i):
        string = ""
        for param_name in self.hyper_vars:
            string += "%s: %s\n" % (param_name, str(self.param_lst[i][param_name]))
        return string

    def get_name_for(self, i):
        out = self.cirrus_objs[i].get_name()
        return out

    def get_cost(self):
        cost = 0
        for i in range(len(self.param_lst)):
            c = self.cirrus_objs[i]
            cost += c.cost_model.get_cost(time.time() - self.start_time)
        return cost

    def get_cost_per_sec(self):
        return sum([c.cost_model.get_cost_per_second() for c in self.cirrus_objs])

    def get_num_lambdas(self):
        return sum([c.get_num_lambdas(fetch=False) for c in self.cirrus_objs])

    # Gets x-axis values of specified metric from experiment i 
    def get_xs_for(self, i, metric="LOSS"):
        if metric == "LOSS":
            lst = self.loss_lst[i]
        elif metric == "UPS":
            lst = self.cirrus_objs[i].get_updates_per_second(fetch=False)
        elif metric == "CPS":
            lst = self.cirrus_objs[i].get_cost_per_second()
        else:
            raise Exception('Metric not available')
        return [item[0] for item in lst]

    # Helper method that collapses a list of commands into a single one
    def get_total_command(self):
        cmd_lst = []
        for c in self.cirrus_objs:
            cmd_lst.append(c.get_command())
        return ' '.join(cmd_lst)

    # TODO: Fix duplicate methods
    def get_ys_for(self, i, metric="LOSS"):
        if metric == "LOSS":
            lst = self.loss_lst[i]
        elif metric == "UPS":
            lst = self.cirrus_objs[i].get_updates_per_second(fetch=False)
        elif metric == "CPS":
            lst = self.cirrus_objs[i].get_cost_per_second()
        else:
            raise Exception('Metric not available')
        return [item[1] for item in lst]

    def start_queue_threads(self):
        # Function that checks each experiment to restore lambdas, grab metrics
        def custodian(cirrus_objs, thread_id, num_jobs):
            index = thread_id
            logging.info("Custodian number %d starting..." % thread_id)
            start_time = time.time()

            time.sleep(5)  # HACK: Sleep for 5 seconds to wait for PS to start
            while self.custodians_should_continue:
                cirrus_obj = cirrus_objs[index]

                cirrus_obj.relaunch_lambdas()
                loss = cirrus_obj.get_time_loss()
                self.loss_lst[index] = loss

                print("Thread", thread_id, "exp", index, "loss", self.loss_lst[index])

                index += num_jobs
                if index >= len(cirrus_objs):
                    index = thread_id

                    # Dampener to prevent too many calls at once
                    if time.time() - start_time < 3:
                        time.sleep(3 - time.time() + start_time)
                    start_time = time.time()


        def unbuffer_instance(instance):
            status, stdout, stderr = instance.buffer_commands(False)
            if status != 0:
                print("An error occurred while unbuffering commands on an"
                      " instance. The exit code was %d and the stderr was:"
                      % status)
                print(stderr)
                raise RuntimeError("An error occured while unbuffering"
                                   " commands on an instance.")


        for instance in self.instances:
            instance.buffer_commands(True)

        for cirrus_obj in self.cirrus_objs:
            cirrus_obj.ps.start(cirrus_obj.define_config())

        threads = []
        for instance in self.instances:
            t = threading.Thread(
                target=unbuffer_instance,
                args=(instance,))
            t.start()
            threads.append(t)
        [t.join() for t in threads]

        # Start custodian threads
        self.custodians_should_continue = True
        for i in range(self.num_jobs):
            p = threading.Thread(target=custodian, args=(self.cirrus_objs, i, self.num_jobs))
            p.start()

    def get_number_experiments(self):
        return len(self.cirrus_objs)

    def set_threads(self, n):
        self.num_jobs = n

        self.adjust_num_threads()


    # Start threads to maintain all experiments
    def run(self, UI=False):
        self.start_queue_threads()

        if UI:
            def ui_func(self):
                graph.bundle = self
                graph.app.run_server()

            self.ui_thread = threading.Thread(target=ui_func, args = (self, ))
            self.ui_thread.start()

    # Stop all experiments
    def kill_all(self):
        total_workers = sum(c.n_workers for c in self.cirrus_objs)
        # I add one extra concurrent execution per experiment. I haven't
        #   observed anything that leads me to believe this is necessary - it's
        #   just in case.
        min_concurrency = total_workers + len(self.cirrus_objs)
        if automate.concurrency_limit(setup.LAMBDA_NAME) < min_concurrency:
            raise RuntimeError("The concurrency limit is set too low to run "
                               "these experiments in parallel.")

        for cirrus_ob in self.cirrus_objs:
            cirrus_ob.kill()

        self.custodians_should_continue = False

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
            if len(loss) == 0:
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
