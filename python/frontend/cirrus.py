# Logistic Regression

import threading
import ec2_vm
import paramiko
import time
import os


class LogisticRegressionTask:
    def __init__(self,
            learning_rate,
            epsilon,
            key_name, key_path,
            ps_ip,
            ps_username):
        print("Starting LogisticRegressionTask")
        self.thread = threading.Thread(target=self.run)
        self.key_name = key_name
        self.key_path = key_path
        self.ps_ip = ps_ip
        self.ps_username = ps_username
        self.learning_rate = learning_rate
        self.epsilon = epsilon

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
            # XXX: replace a.pdf with the actual binary
            print("Done waiting... Attempting to copy over binary")
            stdin, stdout, stderr = client.exec_command(\
                "wget https://s3-us-west-2.amazonaws.com/" \
                + "andrewmzhang-bucket/parameter_server && "\
                + "chmod +x parameter_server")

            print "LS"
            stdout.readlines()
            stdin, stdout, stderr = client.exec_command("ls")
            for line in stdout.readlines():
                print(line)
            client.close()
            print "LS done"
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
		  '"nohup ./parameter_server config_lr.txt 10 1 >ps_output 2>&1 &"'
        print("cmd:", cmd)
        os.system(cmd)

    def launch_lambda(self, num_task, num_workers):
        # This code is untested, need to ask Joao how to do this
        client = boto3.client('lambda', region_name='us-west-2')
        response = client.invoke(
            FunctionName="testfunc1",
            LogType='Tail',
            Payload={"num_task": str(num_task), "num_workers": str(num_workers)})

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

            print "Got machine with ip %s" % ip_addr
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

    def wait(self):
        print "waiting"
        return 1,2

    def define_config(self, ip):
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
                 "minibatch_size: 20 \n" + \
                 "learning_rate: %s \n" % self.learning_rate + \
                 "epsilon: %s \n" % self.epsilon + \
                 "model_bits: 19 \n" + \
                 "s3_bucket: cirrus-criteo-kaggle-19b-random \n" + \
                 "use_grad_threshold: 1 \n" + \
                 "grad_threshold: 0.001 \n" + \
                 "train_set: 0-824 \n" + \
                 "test_set: 825-840"

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
            timeout,
            threshold_loss,
            resume_model,
            key_name,
            key_path,
            ps_ip="", ps_username="ec2-user"):
    print "Running Logistic Regression workload"
    return LogisticRegressionTask(
                learning_rate=learning_rate,
                epsilon=epsilon,
                key_name=key_name,
                key_path=key_path,
                ps_ip=ps_ip,
                ps_username=ps_username
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
