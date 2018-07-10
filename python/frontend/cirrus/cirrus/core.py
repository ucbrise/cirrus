# Core model
from abc import ABCMeta, abstractmethod

import threading
import paramiko
import time
import os
import boto3
from threading import Thread
from CostModel import CostModel
import socket
import struct
import time
import random
import messenger

class BaseTask(object):
    __metaclass__ = ABCMeta
    time_loss_lst = []

    def __init__(self,
            n_workers,
            n_ps,
            worker_size,
            dataset,
            learning_rate,
            epsilon,
            key_name, key_path, # aws key
            ps_ip_public, # public parameter server ip
            ps_ip_private, # private parameter server ip
            ps_ip_port,
            ps_username, # parameter server VM username
            opt_method, # adagrad, sgd, nesterov, momentum
            checkpoint_model, # checkpoint model every x seconds
            train_set,
            test_set,
            minibatch_size,
            model_bits,
            use_grad_threshold,
            grad_threshold,
            timeout,
            threshold_loss,
            progress_callback
            ):
        self.thread = threading.Thread(target=self.run)
        self.n_workers = n_workers
        self.n_ps = n_ps
        self.worker_size = worker_size
        self.dataset=dataset
        self.learning_rate = learning_rate
        self.epsilon = epsilon
        self.key_name = key_name
        self.key_path = key_path
        self.ps_ip_public = ps_ip_public
        self.ps_ip_private = ps_ip_private
        self.ps_ip_port = ps_ip_port
        self.ps_username = ps_username
        self.opt_method = opt_method
        self.checkpoint_model = checkpoint_model
        self.train_set=train_set
        self.test_set=test_set
        self.minibatch_size=minibatch_size
        self.model_bits=model_bits
        self.use_grad_threshold=use_grad_threshold
        self.grad_threshold=grad_threshold
        self.timeout=timeout
        self.threshold_loss=threshold_loss
        self.progress_callback=progress_callback
        self.time_loss_lst = []
        self.dead = False
        self.cost_model = None
        self.total_cost = 0
        self.cost_per_second = 0
        self.total_lambda = 0
        self.id = 0

        # HACK: Prevents Cirrus objects from spawning personal threads
        self.personal_thread = False


    def copy_ps_to_vm(self, ip):
        print("Copying ps to vm..")
        # Setup via ssh
        key = paramiko.RSAKey.from_private_key_file(self.key_path)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # Download ps to vm
        try:
            client.connect(hostname=ip, username=self.ps_username, pkey=key)
            # Set up ssm (if we choose to use that, and get the binary)
            # FIXME: make wget replace old copies of parameter_server and not make a new one.
            print("Done waiting... Attempting to copy over binary")
            #stdin, stdout, stderr = client.exec_command(\
            #    "rm -rf parameter_server && " \
            #    + "wget -q https://s3-us-west-2.amazonaws.com/" \
            #    + "cirrus-parameter-server/parameter_server && "\
            #    + "chmod +x parameter_server")
        except Exception, e:
            print "Got an exception in copy_ps_to_vm..."
            print e
        print "Copied PS binary to VM"


    def launch_ps(self, ip):
        print "Launching ps"
        #key = paramiko.RSAKey.from_private_key_file(self.key_path)

        #client = paramiko.SSHClient()
        #self.ssh_client = client
        #client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        #client.connect(hostname=ip, username=self.ps_username, pkey=key)
        # Set up ssm (if we choose to use that, and get the binary) XXX: replace a.pdf with the actual binary
        print "Launching parameter server"

        cmd = 'ssh -o "StrictHostKeyChecking no" -i %s %s@%s ' \
                % (self.key_path, self.ps_username, self.ps_ip_public) + \
		'"nohup ./parameter_server --config config.txt --nworkers 10 --rank 1 --ps_port %d &> ps_output &"' % self.ps_ip_port
        #print("cmd:", cmd)
        os.system(cmd)
        time.sleep(2)

    def kill_all(self):
        print "Launching ps"
        key = paramiko.RSAKey.from_private_key_file(self.key_path)

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=self.ps_ip_public, username=self.ps_username, pkey=key)
        client.exec_command("killall parameter_server")
        time.sleep(2)

    def get_num_lambdas(self):
        #print("Lambdas: ", self.ps_ip_public, self.ps_ip_port)
        return messenger.get_num_lambdas(self.ps_ip_public, self.ps_ip_port)


    def relaunch_lambdas(self):
        # if 0 run indefinitely
        if self.kill_signal.is_set():
            print("Task appears dead...")
            return;

        num_lambdas = self.get_num_lambdas();
        num_task = 3
        if num_lambdas < self.n_workers:
            shortage = num_workers - self.n_workers;

            payload = '{"num_task": %d, "num_workers": %d, "ps_ip": \"%s\", "ps_port": %d}' \
                        % (num_task, num_workers, self.ps_ip_private, self.ps_ip_port)
            for i in range(shortage):
                try:
                    response = client.invoke(
                        FunctionName="myfunc",
                        InvocationType='Event',
                        LogType='Tail',
                        Payload=payload)
                except:
                    print "client.invoke exception caught"

    # FIXME: Refactor below to launch_lambda_threads
    # if timeout is 0 we run lambdas indefinitely
    # otherwise we stop invoking them after timeout secs
    def launch_lambda(self, num_workers, timeout=50):
        print "Launching lambdas"
        client = boto3.client('lambda', region_name='us-west-2')
        start_time = time.time()
        def launch():

            # if 0 run indefinitely
            while ((timeout == 0) or (time.time() - start_time < timeout)) and not self.kill_signal.is_set():
                time.sleep(2)

                if self.kill_signal.is_set():
                    print("Lambda launcher has received kill signal")
                    return;

                # TODO: Make this grab the number of lambdas from PS log
                num_lambdas = self.get_num_lambdas();
                #print "PS has %d lambdas and " % num_lambdas
                # FIXME: Rename num_workers
                num_task = 3
                if num_lambdas < num_workers:
                    #print "Launching more lambdas"
                    # Launch more lambdas

                    payload = '{"num_task": %d, "num_workers": %d, "ps_ip": \"%s\", "ps_port": %d}' \
                                % (num_task, num_workers, self.ps_ip_private, self.ps_ip_port)
                    #print "payload:", payload

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
    		  '"./parameter_server --config config.txt --nworkers 10 --rank 2 --ps_ip \"%s\" --ps_port %d &> /dev/null" &' % (self.ps_ip_private,  self.ps_ip_port)
            print('cmd', cmd)
            os.system(cmd)

            time.sleep(3)

            start_time = time.time()
            cost_model = CostModel(
                    'm5.large',
                    self.n_ps,
                    0,
                    num_workers,
                    self.worker_size)
            self.cost_model = cost_model

            while not self.kill_signal.is_set():
                time.sleep(1)
                elapsed_time = time.time() - start_time
                self.total_cost = cost_model.get_cost(elapsed_time)
                self.cost_per_second = cost_model.get_cost_per_second()
                t, loss = messenger.get_last_time_error(self.ps_ip_public, self.ps_ip_port +1)
                self.progress_callback((float(t), float(loss)), \
                        cost_model.get_cost(elapsed_time), \
                        "task1")

                if self.timeout > 0 and float(t) > self.timeout:
                    #print("error is timing out")
                    return

        self.start_time = time.time()

        if self.personal_thread:
            self.lambda_launcher = Thread(target=launch)
            self.error_task = Thread(target=error_task)
            self.lambda_launcher.start()
            self.error_task.start()
        else:
            print "Avoiding error and lambda threads"

        #print "Lambdas have been launched"

    def get_time_loss(self):
        t, loss = messenger.get_last_time_error(self.ps_ip_public, self.ps_ip_port + 1)
        if (t == 0):
            return []
        if len(self.time_loss_lst) == 0 or not ((t, loss) == self.time_loss_lst[-1]):
            self.time_loss_lst.append((t, loss))
        return self.time_loss_lst

    def get_name(self):
        return str(self.learning_rate)

    def run(self):
        if self.ps_ip_public == "" or self.ps_ip_private == "":
            print "Creating a spot VM"
            # create vm manager
            vm_manager = ec2_vm.Ec2VMManager("ec2 manager", "", "")
            # launch a spot instance
            print "Creating spot instance"
            vm_instance = vm_manager.start_vm_spot(1, self.key_name) # start 1 vm
            self.ps_ip_public = vm_manager.setup_vm_and_wait() # FIXME

            print "Got machine with ip %s" % self.ps_ip_public
            # copy parameter server and binary to instance
            # Using ssh for now
            print("Waiting for VM start")
	    # need to wait until VM and ssh-server starts
            time.sleep(60)
        else:
            print "User's specific ip:", self.ps_ip_public

        self.copy_ps_to_vm(self.ps_ip_public)
        self.define_config(self.ps_ip_public)
        self.launch_ps(self.ps_ip_public)
        self.kill_signal = threading.Event()
        self.launch_lambda(self.n_workers, self.timeout)

    def kill(self):
        self.kill_signal.set()
        self.lambda_launcher.join()
        self.error_task.join()
        #print "Everyone is dead"
        self.dead = True

    def is_dead(self):
        return self.dead

    @abstractmethod
    def define_config(self, ip):
        pass
