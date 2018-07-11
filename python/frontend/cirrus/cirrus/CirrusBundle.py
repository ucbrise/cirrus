from utils import *
import threading
import time  

from multiprocessing import Process

class CirrusBundle:

    # Graph interfacer

    def __init__(self):
        self.cirrus_objs = [] # Stores each singular experiment
        self.infos = []       # Stores metadata associated with each experiment
        self.param_lst = []   # Stores parameters of each experiment
        self.check_queue = [] # Queue for checking error/lambdas for each object
        self.num_jobs = 1     # Number of threads checking check_queue

        self.threads = []

        self.kill_signal = threading.Event()

        pass

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

    # FIXME: Better name...
    def custodian(cirrus_objs, thread_id, num_jobs):
        index = thread_id
        print("Custodian starting...")
        while True:
            time.sleep(1)
            cirrus_objs[index].relaunch_lambdas()
            loss = cirrus_objs[index].get_time_loss()
            print("Machine #", index)
            print(loss)
            index += num_jobs
            index = index % len(cirrus_objs)
            
        print "Thread number %d is exiting" % thread_id

    def start_queue_threads(self):
        def custodian(cirrus_objs, thread_id, num_jobs):
            index = thread_id
            print("Custodian starting...")
            while True:
                time.sleep(1)
                cirrus_objs[index].relaunch_lambdas()
                loss = cirrus_objs[index].get_time_loss()
                print("Machine #", index)
                print(loss)
                index += num_jobs
                index = index % len(cirrus_objs)
            
            print "Thread number %d is exiting" % thread_id
        for i in range(self.num_jobs):
            #thread = threading.Thread(target=self.custodian, args=(i, ))
            #thread.start()
            p = Process(target=custodian, args=(self.cirrus_objs, i, self.num_jobs))
            p.start()


    def get_number_experiments(self):
        return len(self.cirrus_objs)

    def set_jobs(self, n):
        self.num_jobs = n

    def set_existing_machines(self):
        pass

    def run(self):
        self.cirrus_objs[0].kill_all()

        for cirrus_ob in self.cirrus_objs:
            cirrus_ob.run()
        time.sleep(10)
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
            loss = cirrus_obj.get_last_loss()
            lst.append((index, loss))
            index += 1
        lst.sort()
        top = lst[:n]
        return [cirrus_obj.get_time_loss() for cirrus_obj in top]

    def kill(self, i):
        # FIXME: Do not delete the cirrus object
        self.cirrus_objs[i].kill()
        if self.hit:
            del self.cirrus_objs[i]
            self.hit = False
