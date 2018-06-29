# Core model
from abc import ABCMeta, abstractmethod

import threading
import paramiko
import time
import os
import boto3
from threading import Thread
from CostModel import CostModel

class BaseTask:
    __metaclass__ = ABCMeta
    time_loss_lst = []

    def __init__():
        super().__init__()


    def copy_ps_to_vm(self, ip):
        print("Copying ps to vm..")


    def launch_ps(self, ip):
        print "Launching ps"

    def fetch_num_lambdas(self):
        return 1447

    # if timeout is 0 we run lambdas indefinitely
    # otherwise we stop invoking them after timeout secs
    def launch_lambda(self, num_workers, timeout=50):
        print "Launching lambdas"
        client = boto3.client('lambda', region_name='us-west-2')
        start_time = time.time()
        def launch():

            # if 0 run indefinitely
            while (timeout == 0) or (time.time() - start_time < timeout):
                time.sleep(2)

                if self.kill_signal.is_set():
                    print("Lambda launcher has received kill signal")
                    return;

                # TODO: Make this grab the number of lambdas from PS log
                num_lambdas = self.fetch_num_lambdas();
                print "PS has %d lambdas" % num_lambdas
                # FIXME: Rename num_workers
                num_task = 3
                if num_lambdas < num_workers:
                    print "Launching more lambdas"
                    # Launch more lambdas

                    payload = '{"num_task": %d, "num_workers": %d, "ps_ip": \"%s\"}' \
                                % (num_task, num_workers, self.ps_ip_private)
                    print "payload:", payload

                    try:
                        response = client.invoke(
                            #FunctionName="testfunc1",
                            FunctionName="myfunc",
                            InvocationType='Event',
                            LogType='Tail',
                            Payload=payload)
                    except:
                        print "client.invoke exception caught"

        def error_task():
            print "Starting error task"
            cmd = 'ssh -o "StrictHostKeyChecking no" -i %s %s@%s ' \
                    % (self.key_path, self.ps_username, self.ps_ip_public) + \
    		  '"./parameter_server --config config.txt --nworkers 10 --rank 2 --ps_ip \"%s\"" > error_out &' \
                  % self.ps_ip_private
            print('cmd', cmd)
            os.system(cmd)

            while True:
                try:
                    file = open('error_out')
                    break
                except Exception, e:
                    print "Waiting for error task to start"
                    time.sleep(2)


            def tail(file):
                file.seek(0, 2)
                while not file.closed:
                    line = file.readline()
                    if not line:
                        time.sleep(2)
                        continue
                    if self.kill_signal.is_set():
                        yield "END"
                    yield line

            start_time = time.time()
            cost_model = CostModel(
                    'm5.large',
                    self.n_ps,
                    0,
                    num_workers,
                    self.worker_size)

            for line in tail(file):
                if "Loss" in line:
                    s = line.split(" ")
                    loss = s[3].split("/")[1]
                    t = s[-1][:-2] # get time and get rid of newline

                    elapsed_time = time.time() - start_time
                    self.progress_callback((float(t), float(loss)), \
                            cost_model.get_cost(elapsed_time), \
                            "task1")
                    time_loss_lst.append((t, loss))

                    if self.timeout > 0 and float(t) > self.timeout:
                        print("error is timing out")
                        return
                elif "END" in line:
                    print("Error task has received kill signal")
                    return

        self.lambda_launcher = Thread(target=launch)
        self.error_task = Thread(target=error_task)

        self.lambda_launcher.start()
        self.error_task.start()

        print "Lambdas have been launched"

    def get_time_loss(self):
        return time_loss_lst

    def get_name(self):
        return "mock object"

    def run(self):

        self.copy_ps_to_vm(self.ps_ip_public)
        self.define_config(self.ps_ip_public)
        self.launch_ps(self.ps_ip_public)
        self.kill_signal = threading.Event()
        self.launch_lambda(self.n_workers, self.timeout)

    def kill(self):
        self.kill_signal.set()
        self.lambda_launcher.join()
        self.error_task.join()
