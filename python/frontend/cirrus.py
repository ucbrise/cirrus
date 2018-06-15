# Logistic Regression

import threading
import ec2_vm
import paramiko
import time
import os
import boto3
from threading import Thread

class CostModel:
    def __init__(self,
            vm_type,
            num_vms,
            s3_space_mb,
            num_workers,
            worker_size):

        self.vm_type = vm_type
        self.num_vms = num_vms
        self.s3_space_mb = s3_space_mb
        self.num_workers = num_workers
        self.worker_size = worker_size

        print "Cost Model"

    # compute cos ($) of running worload
    # with given number of vms of specific type,
    # with lambdas of specific size
    # and s3 storage of specific size
    def get_cost(self, num_secs):
        # cost of smallest lambda (128MB) per hour
        lambda_cost_base_h = 0.007488
        total_lambda_cost_h = (self.worker_size / 128.0 * lambda_cost_base_h) \
                * self.num_workers
        total_lambda_cost = total_lambda_cost_h / (60 * 60) * num_secs

        # vm_cost
        vm_to_cost = {
            'm5.large' : 0.096 # demand price per hour
        }

        if self.vm_type not in vm_to_cost:
            raise "Unknown VM type"

        total_vm_cost_h = vm_to_cost[self.vm_type] * self.num_vms
        total_vm_cost = total_vm_cost_h / (60 * 60) * num_secs


        # S3 cost
        # s3 costs $0.023 per GB per month
        s3_cost_gb_h = 0.023 / (30 * 24)
        total_s3_cost_h = s3_cost_gb_h * (1.0 * self.s3_space_mb / 1024)
        total_s3_cost = total_s3_cost_h / (60 * 60) * num_secs

        return total_lambda_cost + total_vm_cost + total_s3_cost


class LogisticRegressionTask:
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
        print("Starting LogisticRegressionTask")
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

    def __del__(self):
        print("Logistic Regression Task Lost")

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
            stdin, stdout, stderr = client.exec_command(\
                "rm -rf parameter_server && " \
                + "wget -q https://s3-us-west-2.amazonaws.com/" \
                + "cirrus-parameter-server/parameter_server && "\
                + "chmod +x parameter_server")
        except Exception, e:
            print "Got an exception in copy_ps_to_vm..."
            print e
        print "Copied PS binary to VM"


    def launch_ps(self, ip):
        print "Launching ps"
        key = paramiko.RSAKey.from_private_key_file(self.key_path)

        client = paramiko.SSHClient()
        self.ssh_client = client
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip, username=self.ps_username, pkey=key)
        # Set up ssm (if we choose to use that, and get the binary) XXX: replace a.pdf with the actual binary
        print "Launching parameter server"

        cmd = 'ssh -o "StrictHostKeyChecking no" -i %s %s@%s ' \
                % (self.key_path, self.ps_username, self.ps_ip_public) + \
		'"nohup ./parameter_server --config config_lr.txt --nworkers 10000 --rank 1 &> ps_output &"'
        print("cmd:", cmd)
        client.exec_command("killall parameter_server")
        os.system(cmd)
        time.sleep(2)

    # if timeout is 0 we run lambdas indefinitely
    # otherwise we stop invoking them after timeout secs
    def launch_lambda(self, num_workers, timeout=50):
        print "Launching lambdas"
        client = boto3.client('lambda', region_name='us-west-2')
        start_time = time.time()
        def launch(num_task):
            i = 0

            # if 0 run indefinitely
            while (timeout == 0) or (time.time() - start_time < timeout):
                if i == 1:
                    print "launching lambda with id %d" % num_task
                else:
                    print "relaunching lambda with id %d %d" % (num_task, i)

                payload = '{"num_task": %d, "num_workers": %d, "ps_ip": \"%s\"}' \
                            % (num_task, num_workers, self.ps_ip_private)
                print "payload:", payload

                try:
                    response = client.invoke(
                        FunctionName="testfunc1",
                        #FunctionName="myfunc",
                        LogType='Tail',
                        Payload=payload)
                except:
                    print "client.invoke exception caught"
                time.sleep(2)
            print "Lambda no. %d will stop refreshing" % num_task

        def error_task():
            print "Starting error task"
            cmd = 'ssh -o "StrictHostKeyChecking no" -i %s %s@%s ' \
                    % (self.key_path, self.ps_username, self.ps_ip_public) + \
    		  '"./parameter_server --config config_lr.txt --nworkers 10 --rank 2 --ps_ip \"%s\"" > error_out &' \
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

                    if self.timeout > 0 and float(t) > self.timeout:
                        print("error is timing out")
                        return



        threads = []
        for i in range(3, 3 + num_workers):
            thread = Thread(target=launch, args=(i, ))
            thread.start()
            threads.append(thread)

        error_task()

        print "Waiting for threads"
        for thread in threads:
            thread.join()

        print "Lambdas have been launched"

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
        self.launch_lambda(self.n_workers, self.timeout)

    def wait(self):
        print "waiting"
        return 1,2

    def define_config(self, ip):
        if self.use_grad_threshold:
            grad_t = 1
        else:
            grad_t = 0

        config = "input_path: /mnt/efs/criteo_kaggle/train.csv \n" + \
                 "input_type: csv\n" + \
                 "num_classes: 2 \n" + \
                 "num_features: 13 \n" + \
                 "limit_cols: 14 \n" + \
                 "normalize: 1 \n" + \
                 "limit_samples: 50000000 \n" + \
                 "s3_size: 50000 \n" + \
                 "use_bias: 1 \n" + \
                 "model_type: LogisticRegression \n" + \
                 "minibatch_size: %d \n" % self.minibatch_size + \
                 "learning_rate: %f \n" % self.learning_rate + \
                 "epsilon: %lf \n" % self.epsilon + \
                 "model_bits: %d \n" % self.model_bits + \
                 "s3_bucket: %s \n" % self.dataset + \
                 "use_grad_threshold: %d \n" % grad_t + \
                 "grad_threshold: %lf \n" % self.grad_threshold + \
                 "train_set: %d-%d \n" % self.train_set + \
                 "test_set: %d-%d" % self.test_set

        key = paramiko.RSAKey.from_private_key_file(self.key_path)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip, username=self.ps_username, pkey=key)
        print "Defining configuration file"
        stdin, stdout, stderr = client.exec_command('echo "%s" > config_lr.txt' % config)
        print stdout.read()
        stdin, stdout, stderr = client.exec_command('cat config_lr.txt')
        print stdout.read()
        client.close()

def dataset_handle(path, format):
    print "path: ", path, " format: ", format
    return 0

def LogisticRegression(
            n_workers,
            n_ps,
            worker_size,
            dataset,
            learning_rate, epsilon,
            progress_callback,
            resume_model,
            key_name,
            key_path,
            train_set,
            test_set,
            minibatch_size,
            model_bits,
            ps_ip_public="",
            ps_ip_private="",
            ps_username="ec2-user",
            opt_method="sgd",
            checkpoint_model=0,
            use_grad_threshold=False,
            grad_threshold=0.001,
            timeout=60,
            threshold_loss=0
            ):
    print "Running Logistic Regression workload"
    return LogisticRegressionTask(
            n_workers=n_workers,
            n_ps=n_ps,
            worker_size=worker_size,
            dataset=dataset,
            learning_rate=learning_rate,
            epsilon=epsilon,
            key_name=key_name,
            key_path=key_path,
            ps_ip_public=ps_ip_public,
            ps_ip_private=ps_ip_private,
            ps_username=ps_username,
            opt_method=opt_method,
            checkpoint_model=checkpoint_model,
            train_set=train_set,
            test_set=test_set,
            minibatch_size=minibatch_size,
            model_bits=model_bits,
            use_grad_threshold=use_grad_threshold,
            grad_threshold=grad_threshold,
            timeout=timeout,
            threshold_loss=threshold_loss,
            progress_callback=progress_callback
           )

def create_random_lr_model(n):
    #print "Creating generic model not yet implemented, creating criteo kaggle model"
    print "Creating random LR model with size: ", n

    return 0

# Collaborative Filtering
def CollaborativeFiltering():
    print "not implemented"


# LDA algorithm
def LDA():
    print "Not implemented"
