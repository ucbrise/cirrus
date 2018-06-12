# Logistic Regression

import threading
import ec2_vm
import paramiko
import time

class LogisticRegressionTask:
    def __init__(self):
        print "Starting LogisticRegressionTask"
        self.thread = threading.Thread(target=self.run)

    def __del__(self):
        print "Logistic Regression Task Lost. Closing ssh connection"
        self.client.close();

    def copy_driver_to_vm(self, ip):
        print "Copying driver to vm"
        # Setup via ssh
        key = paramiko.RSAKey.from_private_key_file("/home/camus/Downloads/mykey.pem")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # Download driver to vm
        try:
            print "Waiting for ssh startup"
            time.sleep(60)  # SSH doesn't immediately work, it helps if you wait for a minute before trying
            client.connect(hostname=ip, username='ubuntu', pkey=key)
            # Set up ssm (if we choose to use that, and get the binary) XXX: replace a.pdf with the actual binary
            print "Done waiting... Attempting to copy over binary"
            stdin, stdout, stderr = client.exec_command("wget https://s3-us-west-2.amazonaws.com/andrewmzhang-bucket/parameter_server && chmod +x parameter_server")
            stdout.readlines()
            stdin, stdout, stderr = client.exec_command("ls")
            for line in stdout.readlines():
                print line
            client.close()
        except Exception, e:
            print "Got an exception..."
            print e


    def launch_driver(self, ip):
        print "Launching driver"
        key = paramiko.RSAKey.from_private_key_file("/home/camus/Downloads/mykey.pem")

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        time.sleep(10)

        client.connect(hostname=ip, username='ubuntu', pkey=key)
        # Set up ssm (if we choose to use that, and get the binary) XXX: replace a.pdf with the actual binary
        print "Launching parameter server"
        #stdin, stdout, stderr = client.exec_command("./parameter_server config_lr.txt 100 1 &") # Not sure if there's a good way to do this....
        #client.close()
        transport = client.get_transport()
        channel = transport.open_session()
        channel.exec_command("./parameter_server config_lr.txt 100 1 |& tee ps_log.txt &")

        client.close()
    def issue_ssh_command(self, command, ip):
        print "Issuing command: %s on %s" % (command, ip)


    def run(self):
        # launch instances
        vm_manager = ec2_vm.Ec2VMManager("ec2 manager", "", "")
        vm_instance = vm_manager.start_vm_spot(1) # start 1 vm
        ip_addr = vm_manager.setup_vm()

        print "Got machine with ip %s" % ip_addr
        # copy driver and binary to instance
        # Using ssh for now

        self.copy_driver_to_vm(ip_addr)
        self.define_config(ip_addr)
        self.launch_driver(ip_addr)

        # launch driver in vm
        #launch_driver(ip)

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
                 "learning_rate: 0.01 \n" + \
                 "epsilon: 0.0001 \n" + \
                 "model_bits: 19 \n" + \
                 "s3_bucket: cirrus-criteo-kaggle-19b-random \n" + \
                 "use_grad_threshold: 1 \n" + \
                 "grad_threshold: 0.001 \n" + \
                 "train_set: 0-824 \n" + \
                 "test_set: 825-840"

        key = paramiko.RSAKey.from_private_key_file("/home/camus/Downloads/mykey.pem")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip, username='ubuntu', pkey=key)
        print "Defining configuration file"
        stdin, stdout, stderr = client.exec_command('echo "%s" > config_lr.txt' % config)
        print stdout.read()
        stdin, stdout, stderr = client.exec_command('cat config_lr.txt')
        print stdout.read()
        client.close()

def dataset_handle(path, format):
    print "path: ", path, " format: ", format
    return 0


def LogisticRegression(n_workers, n_ps,
            dataset,
            access_key,
            secret_key,
            learning_rate, epsilon,
            progress_callback,
            timeout,
            threshold_loss,
            resume_model):
    print "Running Logistic Regression workload"
    return LogisticRegressionTask(
                access_key=access_key,
                secret_key=secret_key
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
