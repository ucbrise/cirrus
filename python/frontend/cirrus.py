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
        ec2_vm.wait_until_running()
        vm_instance = vm_manager.start_vm(1) # start 1 vm
        # copy driver and binary to instance

        ip = ec2_vm.get_instance_ip(vm_instance)
        copy_driver_to_vm(ip)
        # launch driver in vm
        launch_driver(ip)

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

