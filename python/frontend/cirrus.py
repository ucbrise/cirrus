# Logistic Regression

import threading
import ec2_vm

class LogisticRegressionTask:
    def __init__(self):
        print "Starting LogisticRegressionTask"
        self.thread = threading.Thread(target=self.run)

    def copy_driver_to_vm(self, ip):
        print "Copying driver to vm"

    def launch_driver(self, ip):
        print "Launching driver"

    def run(self):
        # launch instances
        vm_manager = ec2_vm.Ec2VMManager("ec2 manager")
        vm_instance = vm_manager.start_vm(1) # start 1 vm
        vm_instance.wait_until_running() # wait for instance to run

        # copy driver and binary to instance
        # Using ssh for now

        vm_instance.wait_until_running() # Wait for vm to run
        vm_instance.load() # wait for settings to update
        ip = vm_instance.public_dns_name # grab the public ip of the vm
        print "Got vm with ip %s" % ip

        key = paramiko.RSAKey.from_private_key_file("/home/camus/Downloads/mykey.pem")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Download driver to vm
        try:
            print "Waiting for ssh startup"
            time.sleep(10)
            print "Done waiting..."
            client.connect(hostname=ip, username='ubuntu', pkey=key)
            # Set up ssm (if we choose to use that, and get the binary) XXX: replace a.pdf with the actual binary
            stdin, stdout, stderr = client.exec_command("wget https://s3.amazonaws.com/ec2-downloads-windows/SSMAgent/latest/debian_amd64/amazon-ssm-agent.deb && sudo dpkg -i amazon-ssm-agent.deb")
            stdin, stdout, stderr = client.exec_command("aws s3 cp s3://andrewmzhang-bucket/a.pdf")
            print stdout.read()
            client.close()
        except Exception, e:
            print e


        client_ssm = boto3.client('ssm')
        commands = ['echo "hello world"']  # XXX: replace this with the execute binary command
        instance_ids = [vm_instance.instance_id]

        # XXX: figure out what to do with this thing
        resp  = client_ssm.send_command(
            DocumentName='AWS-RunShellScript',
            Parameters={'commands': commands},
            InstanceIds = instance_ids
        )

        # launch driver in vm
        #launch_driver(ip)

    def wait(self):
        print "waiting"
        return 1,2

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
    print "Creating random LR model with size: ", n
    return 0

# Collaborative Filtering

def CollaborativeFiltering():
    print "not implemented"


# LDA algorithm


def LDA():
    print "Not implemented"
