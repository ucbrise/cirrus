# file used to manage VMs in Amazon EC2

import boto3
import time
import paramiko

class Ec2VMManager:
    def __init__(self, description, access_key, secret_key):
      print "Starting VM Manager"
      self.description = description
      self.ec2_client = boto3.client('ec2', region_name='us-west-2')
      self.ec2_resource = boto3.resource('ec2', region_name='us-west-2')
      self.access_key = access_key
      self.secret_key = secret_key
      self.instances = [] # vm instances managed by the manager

    def start_vm(self, number_vms, key_name):
      assert(number_vms == 1) # for now we only support 1 vm
      print "Starting EC2 vm"
      tags = [
               {'Key':'runtime','Value': 'Cirrus 0.1'},
               {'Key':'owner', 'Value': 'Cirrus'},
             ]
      tag_specification = [{'ResourceType': 'instance', 'Tags': tags},]

      print "Starting instance"
      print "ImageId: ami-db710fa3", " InstanceType: m5.large"
      instance = self.ec2_resource.create_instances(
         # Ubuntu Server 16.04 LTS (HVM), SSD Volume Type - ami-db710fa3
         ImageId='ami-db710fa3',
         MinCount=1,
         MaxCount=1,
         InstanceType='t2.micro',
         KeyName=key_name,
         TagSpecifications=tag_specification
      )
      self.instances.append(instance[0])

      return instance[0] # return instance object

    def start_vm_spot(self, number_vms, key_name, price="0.01"):
        assert(number_vms == 1)
        print "Starting Spot Instance Request for %d VMs at price %s" % (number_vms, price)
        client = self.ec2_client
        rc = client.request_spot_instances(
                        DryRun=False,
                        SpotPrice="0.01",
                        Type='one-time',
                        InstanceCount=1,
                        LaunchSpecification={'ImageId': 'ami-d26724aa',
                                             'InstanceType': 'm1.small',
                                             'KeyName': key_name
                                             })
        state = 'open'
        request_id = rc[u'SpotInstanceRequests'][0][u'SpotInstanceRequestId']
        instance_id = None
        print "Created Spot Request w/ ID: %s" % request_id

        while state == 'open':
            print "Waiting on request..."
            time.sleep(10)
            spot = client.describe_spot_instance_requests(DryRun = False, SpotInstanceRequestIds=[request_id])
            state = spot[u'SpotInstanceRequests'][0][u'State']
            print "Current state is: %s" % state
        instance_id = spot[u'SpotInstanceRequests'][0][u'InstanceId']
        print "Spot request granded with ID: %s" % instance_id

        print "Spot instance created! Instance id is: %s" % instance_id
        tags = [{'Key':'runtime','Value': 'Cirrus 0.1'}, {'Key':'owner', 'Value': 'Cirrus'}]
        for instance in self.ec2_resource.instances.all():
            if instance.instance_id == instance_id:
                client.create_tags(Resources=[instance_id], Tags=tags)
                self.instances.append(instance)
                return instance
        print "Unexpected error in finding instance..."


    def stop_all_vms(self):
      print "stopping all vms"
      #return # don't stop any vms until we
      #  understand whats going on
      for instance_id in self.instances:
        instance = self.ec2_resource.instances.filter(InstanceIds=[instance_id.instance_id]).stop()
        response = self.ec2_resource.instances.filter(InstanceIds=[instance_id.instance_id]).terminate()
        print response

    def setup_vm_and_wait(self):
      print "setup vm"
      vm_instance = self.instances[0]
      vm_instance.wait_until_running() # Wait for vm to run
      vm_instance.load() # wait for settings to update
      ip = vm_instance.public_dns_name # grab the public ip of the vm
      return ip


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


    def wait_until_running(self, vm_instance):
      instance_id = vm_instance[0]
      while True:
        for instance in self.ec2_resource.instances.all():
          print "Comparing ", instance_id, " with", instance.id
          if instance.id == instance_id:
            print "Checking state: ", instance.state
            if instance.state['Name'] == 'running':
              return
        time.sleep(1)
