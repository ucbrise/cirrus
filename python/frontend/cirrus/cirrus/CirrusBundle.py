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

    def __init__(self):
        self.cirrus_objs = [] # Stores each singular experiment
        self.infos = []       # Stores metadata associated with each experiment
        self.param_lst = []   # Stores parameters of each experiment
        self.check_queue = [] # Queue for checking error/lambdas for each object
        self.num_jobs = 1     # Number of threads checking check_queue
        self.set_timeout = -1 # Timeout. -1 means never timeout

        self.threads = []

        self.kill_signal = threading.Event()
        self.loss_lst = []

    def get_xs_for(self, i):
        lst = self.loss_lst[i]
        print("Current", self.loss_lst)
        return [item[0] for item in lst]
    
    def get_ys_for(self, i):
        lst = self.loss_lst[i]
        return [item[1] for item in lst]
        

    def set_task_parameters(self, task, param_dict_lst):
        self.param_lst = param_dict_lst
        index = 0
        base_port = 1337
        for param in param_dict_lst:
            param['ps_ip_port'] = base_port + (index * 2)
            index += 1
            c = task(**param)
            self.cirrus_objs.append(c)
            self.infos.append({'color': get_random_color()})


    def start_queue_threads(self):
        def custodian(cirrus_objs, thread_id, num_jobs, infos):
            index = thread_id
            logging.info("Custodian number %d starting..." % thread_id)
            seen = []
            start_time = time.time()
            while True:
                cirrus_obj = cirrus_objs[index]
                if not (index in seen):
                    self.run(index)
                    seen.append(index)
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
        
        for _ in range(self.get_number_experiments()):
            self.loss_lst.append([])

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
        assert(0 <= index <= len(self.cirrus_objs))
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
        if self.hit:
            del self.cirrus_objs[i]
            self.hit = False
