from utils import *
import threading
import time

from multiprocessing import Process
from multiprocessing import Process, Manager
from multiprocessing.managers import BaseManager

import logging


logging.basicConfig(filename="cirrusbundle.log", level=logging.WARNING)

class CirrusBundle:

    # Graph interfacer


    # TODO: Add some sort of optional argument checking
    def __init__(self, task=None, param_dict_lst=None, param_base=None, hyper_vars=[], hyper_params=[], num_jobs=1, timeout=-1):
        
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

        # Setup
        self.set_task_parameters(
                task, 
                param_dict_lst=param_dict_lst, 
                param_base=param_base, 
                hyper_vars=hyper_vars, 
                hyper_params=hyper_params)

    def get_info_for(self, i):
        return "Learning Rate: %f" % self.cirrus_objs[i].learning_rate;

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

    def get_xs_for(self, i):
        lst = self.loss_lst[i]
        return [item[0] for item in lst]
    
    def get_ys_for(self, i):
        lst = self.loss_lst[i]
        return [item[1] for item in lst]
        

    # TODO: Add some sort of optional argument checking
    # User must either specify param_dict_lst, or hyper_vars, hyper_params, and param_base
    def set_task_parameters(self, task, param_dict_lst=[], param_base=None, hyper_vars=[], hyper_params=[]):

        # We have a hyper_var thing
        if len(hyper_vars) > 0:
            self.param_lst = []
            base_port = 1337
            for params in hyper_params:
                param_base_cpy = param_base.copy()
                for var_param in zip(hyper_vars, params):
                    var, param = var_param
                    param_base_cpy[var] = param
                param_base_cpy['ps_ip_port'] = base_port
                c = task(**param)
                self.cirrus_objs.append(c)
                self.infos.append({'color': get_random_color()})
                self.loss_lst.append({})
                base_port += 2
                self.param_lst.append(param_base_cpy)
            return
        else:
            self.param_lst = param_dict_lst
            index = 0
            base_port = 1337
            for param in param_dict_lst:
                param['ps_ip_port'] = base_port + (index * 2)
                index += 1
                c = task(**param)
                self.cirrus_objs.append(c)
                self.infos.append({'color': get_random_color()})
                self.loss_lst.append({})


    def start_queue_threads(self):
        def custodian(cirrus_objs, thread_id, num_jobs, infos):
            index = thread_id
            logging.info("Custodian number %d starting..." % thread_id)
            seen = []
            start_time = time.time()
            while True:
                cirrus_obj = cirrus_objs[index]
                #if not (index in seen):
                #    self.run(index)
                #    seen.append(index)
                cirrus_objs[index].relaunch_lambdas()
                loss = cirrus_objs[index].get_time_loss()
                print("Thread", thread_id, "Machine", index, "Loss", loss)
                self.loss_lst[index] = loss
                index += num_jobs
                if index >= len(cirrus_objs):
                    index = thread_id
                    if time.time() - start_time < 3:
                        time.sleep(3)
                    start_time = time.time()

            logging.info("Thread number %d is exiting" % thread_id)

        def runner(i):
            while i < self.get_number_experiments():
                self.run(i)
                print "Ran %d" % i
                i += 4

        plst = []
        for i in range(4):
            p = threading.Thread(target=runner, args=(i,))
            p.start()
            plst.append(p)

        for i in range(4):
            plst[i].join()
        
        print "Everything started"
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
        self.cirrus_objs[0].kill_all()
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
        # FIXME: Do not delete the cirrus object
        self.cirrus_objs[i].kill()


