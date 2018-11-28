import itertools
import logging
import os
import threading
import time
import boto3
import math

import graph
from utils import *

logging.basicConfig(filename="cirrusbundle.log", level=logging.WARNING)

# NOTE: This is a temporary measure. Ideally this zip would be on the cloud.
# Due to constant updates to bundle.zip, its more convienient to have it local

bundle_zip_location="/home/camus/code/cirrus-1/python/frontend/cirrus/cirrus/bundle.zip"

class GridSearch:


    # TODO: Add some sort of optional argument checking
    def __init__(self,
                 task=None,
                 param_base=None,
                 hyper_vars=[],
                 hyper_params=[],
                 machines=[],
                 num_jobs=1,
                 timeout=-1,
                 ):

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

        ips = []
        for public_dns in machines:
            private_ip = public_dns_to_private_ip(public_dns)
            ips.append(private_ip)
        print ips

        self.machines = zip(machines, ips)

        # Setup
        self.set_task_parameters(
                task,
                param_base=param_base,
                hyper_vars=hyper_vars,
                hyper_params=hyper_params,
                machines=self.machines)


        self.adjust_num_threads();

    def adjust_num_threads(self):
        # make sure we don't have more threads than experiments
        self.num_jobs = min(self.num_jobs, len(self.cirrus_objs))


    # User must either specify param_dict_lst, or hyper_vars, hyper_params, and param_base
    def set_task_parameters(self, task, param_base=None, hyper_vars=[], hyper_params=[], machines=[]):
        possibilities = list(itertools.product(*hyper_params))
        base_port = 1337
        index = 0
        num_machines = len(machines)

        lambdas = get_all_lambdas()

        for p in possibilities:
            configuration = zip(hyper_vars, p)
            modified_config = param_base.copy()
            for var_name, var_value in configuration:
                modified_config[var_name] = var_value
            modified_config['ps_ip_port'] = base_port
            modified_config['ps_ip_public'] = machines[index][0]
            modified_config['ps_ip_private'] = machines[index][1]

            index = (index + 1) % num_machines
            base_port += 2

            c = task(**modified_config)
            self.cirrus_objs.append(c)
            self.infos.append({'color': get_random_color()})
            self.loss_lst.append({})
            self.param_lst.append(modified_config)
            lambda_name = "testfunc1_%d" % c.worker_size
            if not lambda_exists(lambdas, lambda_name, c.worker_size, bundle_zip_location):
                print lambda_name + " Does not exist"
                lambdas.append({'FunctionName': lambda_name})
                create_lambda(bundle_zip_location, size=c.worker_size)

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
    def get_xs_for(self, i, metric):
        lst = self.cirrus_objs[i].fetch_metric(metric)
        return [item[0] for item in lst]

    # Helper method that collapses a list of commands into a single one
    def get_total_command(self):
        cmd_lst = []
        for c in self.cirrus_objs:
            cmd_lst.append(c.get_command())
        return ' '.join(cmd_lst)

    # TODO: Fix duplicate methods
    def get_ys_for(self, i, metric):
        lst = self.cirrus_objs[i].fetch_metric(metric)
        return [item[1] for item in lst]

    def start_queue_threads(self):

        # Function that checks each experiment to restore lambdas, grab metrics
        def custodian(cirrus_objs, thread_id, num_jobs):
            index = thread_id
            logging.info("Custodian number %d starting..." % thread_id)
            start_time = time.time()

            time.sleep(5)  # HACK: Sleep for 5 seconds to wait for PS to start
            while True:
                cirrus_obj = cirrus_objs[index]

                cirrus_obj.relaunch_lambdas()
                loss = cirrus_obj.get_time_loss()
                self.loss_lst[index] = loss

                logging.info("Thread", thread_id, "exp", index, "loss", self.loss_lst[index])


                round_loss_lst = [(round(a, 3), round(float(b), 4))
                        for (a,b) in self.loss_lst[index]]
                logging.debug("Thread", thread_id, "exp", index,
                        "loss", round_loss_lst)
                
                index += num_jobs
                if index >= len(cirrus_objs):
                    index = thread_id

                    time.sleep(0.5)
                    start_time = time.time()

        # Dictionary of commands per machine
        command_dict = {}
        for machine in self.machines:
            command_dict[machine[0]] = []

        # Grab commands each machine needs to run
        for c in self.cirrus_objs:
            c.get_command(command_dict)

        # Write those commands into bash files
        command_dict_to_file(command_dict)

        # Number of threads
        copy_threads = min(len(self.machines), self.num_jobs)

        # Copies bash files to machines and starts experiment
        def copy_and_run(thread_id):
            while True:
                if thread_id >= len(self.machines):
                    return

                sh_file = "machine_%d.sh" % thread_id
                ubuntu_machine = "ubuntu@%s" % self.machines[thread_id][0]

                cmd = "scp %s %s:~/" % (sh_file, ubuntu_machine)
                os.system(cmd)
                cmd = 'ssh %s "killall parameter_server; chmod +x %s; ./%s &"' % (ubuntu_machine, sh_file, sh_file)
                os.system(cmd)
                thread_id += copy_threads

        p_lst = []
        for i in range(copy_threads):
            p = threading.Thread(target=copy_and_run, args=(i,))
            p.start()
            p_lst.append(p)

        [p.join() for p in p_lst]

        # Start custodian threads
        for i in range(self.num_jobs):
            p = threading.Thread(target=custodian, args=(self.cirrus_objs, i, self.num_jobs))
            p.start()

    def get_number_experiments(self):
        return len(self.cirrus_objs)

    def set_threads(self, n):
        


        self.num_jobs = min(n, self.get_number_experiments())

        self.adjust_num_threads();


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
