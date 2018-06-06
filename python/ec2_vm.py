# file used to manage VMs in Amazon EC2

import boto3

class VMManager:
    def __init__(self, description):
        print "Starting VM Manager"
        self.ec2_client = boto3.client('ec2')
        self.instances = []

    def start_vm(self):
        print "starting vm"
        instance = ec2.create_instances(
           ImageId='?',
           MinCount=1,
           MaxCount=1,
           InstanceType='m5.large',
           TagSpecifications=[{'Tags': ['cirrus']}]
        )
        self.instances.append(instance)

    def stop_vms(self):
        print "stopping all vms"
        return # don't stop any vms until we
        #  understand whats going on
        for instance_id in self.instances:
          instance = self.ec2_client.Instance(instance[0])
          response = instance.terminate()
          print response

    def setup_vm(self):
        print "setup vm"

    def get_tags(self, fid):
	# When given an instance ID as str e.g. 'i-1234567', 
    ec2instance = self.ec2_client.Instance(fid)
    return ec2instance.tags
