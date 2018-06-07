# file used to manage VMs in Amazon EC2

import boto3
import time

class Ec2VMManager:
    def __init__(self, description, access_key, secret_key):
      print "Starting VM Manager"
      self.description = description
      self.ec2_client = boto3.client('ec2')
      self.ec2_resource = boto3.resource('ec2')
      self.access_key = access_key
      self.secret_key = secret_key
      self.instances = [] # vm instances managed by the manager

    def start_vm(self):
      print "Starting EC2 vm"
      tags = [
               {'Key':'runtime','Value': 'Cirrus 0.1'},
               {'Key':'owner', 'Value': 'Cirrus'},
             ]
      tag_specification = [{'ResourceType': 'instance', 'Tags': tags},]
      instance = self.ec2_resource.create_instances(
         # Ubuntu Server 16.04 LTS (HVM), SSD Volume Type - ami-db710fa3
         ImageId='ami-db710fa3',
         MinCount=1,
         MaxCount=1,
         InstanceType='m5.large',
         TagSpecifications=tag_specification
      )
      self.instances.append(instance)

      print "instance:", instance[0]
      return instance[0].id

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
      ec2instance = self.ec2_resource.Instance(fid)
      return ec2instance.tags

    def list_all_vm_instances(self):
      for instance in self.ec2_resource.instances.all():
         print instance.id, instance.state

    def list_vm_instances_with_tag(self, tag):
      for instance in self.ec2_resource.instances.all():
        tags = self.get_tags(instance.id)
        print instance.id, ":", tags

    def wait_until_running(self, instance_id):
      while True:
        for instance in self.ec2_resource.instances.all():
          print "Comparing ", instance_id, " with", instance.id
          if instance.id == instance_id:
            print "Checking state: ", instance.state
            if instance.state['Name'] == 'running':
              return
        time.sleep(1)


