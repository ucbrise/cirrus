# Logistic Regression

import threading
import ec2_vm
import paramiko
import time
import os
import boto3
from threading import Thread

class LogisticRegressionTask:
    def __init__(self,
            dataset,
            learning_rate,
            epsilon,
            key_name, key_path, # aws key
            ps_ip, # parameter server ip
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

        self.dataset=dataset
        self.learning_rate = learning_rate
        self.epsilon = epsilon
        self.key_name = key_name
        self.key_path = key_path
        self.ps_ip = ps_ip
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
        print("Logistic Regression Task Lost. Closing ssh connection")
        self.client.close();

    def copy_ps_to_vm(self, ip):
        print("Copying ps to vm")
        # Setup via ssh
        # XXX IMO this shouldn't be necessary if the user as done
        # the aws config (that generates credentials in ~/.aws)
        key = paramiko.RSAKey.from_private_key_file(self.key_path)
        client = paramiko.SSHClient()
        #ssh.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # Download ps to vm
        try:
            client.connect(hostname=ip, username=self.ps_username, pkey=key)
            # Set up ssm (if we choose to use that, and get the binary)
            # FIXME: make wget replace old copies of parameter_server and not make a new one.
            print("Done waiting... Attempting to copy over binary")
            stdin, stdout, stderr = client.exec_command(\
                "wget https://s3-us-west-2.amazonaws.com/" \
                + "andrewmzhang-bucket/parameter_server && "\
                + "chmod +x parameter_server")

        except Exception, e:
            print "Got an exception in copy_ps_to_vm..."
            print e
        print "Copied parameter server"


    def launch_ps(self, ip):
        print "Launching ps"
        key = paramiko.RSAKey.from_private_key_file(self.key_path)

        client = paramiko.SSHClient()
        self.ssh_client = client
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip, username=self.ps_username, pkey=key)
        # Set up ssm (if we choose to use that, and get the binary) XXX: replace a.pdf with the actual binary
        print "Launching parameter server"

        cmd = 'ssh -o "StrictHostKeyChecking no" -i %s %s@%s ' % (self.key_path, self.ps_username, self.ps_ip) + \
		  '"nohup ./parameter_server config_lr.txt 10000 1 &> ps_output &"'
        print("cmd:", cmd)
        client.exec_command("killall parameter_server")
        os.system(cmd)
        time.sleep(2)

    def launch_lambda(self, num_workers, timeout=100):
        print "Launching lambdas"
        client = boto3.client('lambda', region_name='us-west-2')
        start_time = time.time()
        def launch(num_task):
            i = 0

            while time.time() - start_time < timeout:
                if i == 1:
                    print "launching lambda with id %d" % num_task
                else:
                    print "relaunching lambda with id %d %d" % (num_task, i)

                response = client.invoke(
                    FunctionName="myfunc",
                    LogType='Tail',
                    Payload='{"num_task": %d, "num_workers": %d}' % (num_task, num_workers))
                time.sleep(2)
            print "Lambda no. %d will stop refreshing" % num_task

        def error_task():
            cmd = 'ssh -o "StrictHostKeyChecking no" -i %s %s@%s ' % (self.key_path, self.ps_username, self.ps_ip) + \
    		  '"./parameter_server config_lr.txt 10 2" > error_out &'
            print('cmd', cmd)
            os.system(cmd)
            import time
            time.sleep(2)
            ssh_client = paramiko.SSHClient()
            key = paramiko.RSAKey.from_private_key_file(self.key_path)
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.ps_ip, username=self.ps_username, pkey=key)
            sftp_client = ssh_client.open_sftp()
            file = open('error_out')

            def tail(file):
                file.seek(0, 2)
                while not file.closed:
                    line = file.readline()
                    if not line:
                        time.sleep(2)
                        continue
                    yield line

            for line in tail(file):
                if "Loss" in line:
                    s = line.split(" ")
                    loss = s[3].split("/")[1]
                    import string
                    t = s[-1][:-2]
                    self.progress_callback((float(t), float(loss)), 0.1, "task1")

                    if float(t) > 100:
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



    def issue_ssh_command(self, command, ip):
        print "Issuing command: %s on %s" % (command, ip)

    def run(self):
        if self.ps_ip == "":
            print "Creating a spot VM"
            # create vm manager
            vm_manager = ec2_vm.Ec2VMManager("ec2 manager", "", "")
            # launch a spot instance
            print "Creating spot instance"
            vm_instance = vm_manager.start_vm_spot(1, self.key_name) # start 1 vm
            self.ps_ip = vm_manager.setup_vm_and_wait()

            print "Got machine with ip %s" % self.ps_ip
            # copy parameter server and binary to instance
            # Using ssh for now
            print("Waiting for VM start")
	    # need to wait until VM and ssh-server starts
            time.sleep(60)
        else:
            print "User's specific ip:", self.ps_ip

        self.copy_ps_to_vm(self.ps_ip)
        self.define_config(self.ps_ip)
        self.launch_ps(self.ps_ip)
        self.launch_lambda(2)

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
            n_workers, n_ps,
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
            ps_ip="", ps_username="ec2-user",
            opt_method="sgd",
            checkpoint_model=0,
            use_grad_threshold=False,
            grad_threshold=0.001,
            timeout=0,
            threshold_loss=0
            ):
    print "Running Logistic Regression workload"
    return LogisticRegressionTask(
            dataset=dataset,
            learning_rate=learning_rate,
            epsilon=epsilon,
            key_name=key_name,
            key_path=key_path,
            ps_ip=ps_ip,
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
