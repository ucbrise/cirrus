#!/usr/bin/env python
import boto3
ec2 = boto3.resource('ec2')
for instance in ec2.instances.all():
   print instance.id, instance.state

#response = ec2.describe_instances()
#print(response)
