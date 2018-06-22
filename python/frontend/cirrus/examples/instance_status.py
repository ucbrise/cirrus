import boto3

ec2 = boto3.client('ec2')

for status in ec2.meta.client.describe_instance_status()['InstanceStatuses']:
  print(status)
