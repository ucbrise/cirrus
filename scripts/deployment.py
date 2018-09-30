"""Tools for deploying an instance of Cirrus.
"""
import json
import subprocess
import random
import time
import socket

import boto3
from botocore.exceptions import ClientError
import paramiko

# Specifications for a compilation EC2 instance.
REGION = "us-west-1"
DISK_SIZE = 16  # GB
AMI_ID = "ami-3a674d5a"  # This is amzn-ami-hvm-2017.03.1.20170812-x86_64-gp2,
                         #  which is recommended by AWS as of Sep 27, 2018 for
                         #  compiling executables for Lambda.
INSTANCE_TYPE = "t3.xlarge"
SSH_USERNAME = "ec2-user"

# A series of commands that will result in ~/cirrus/src containing Cirrus'
#   compiled executables. Note that each command is issued separately, so
# 	working directory changes will not persist.
COMMANDS = """
yes | sudo yum install git
git clone https://github.com/jcarreira/cirrus
yes | sudo yum install glibc-static
yes | sudo yum install openssl-static.x86_64
yes | sudo yum install zlib-static.x86_64
yes | sudo yum install libcurl-devel
yes | sudo yum groupinstall "Development Tools"
yes | sudo yum remove gcc48-c++
yes | sudo yum install gcc72-c++
wget https://cmake.org/files/v3.10/cmake-3.10.0.tar.gz
tar -xvzf cmake-3.10.0.tar.gz
cd cmake-3.10.0; ./bootstrap
cd cmake-3.10.0; make
cd cmake-3.10.0; sudo make install
cd cirrus; ./bootstrap.sh
cd cirrus; make -j 10
"""

# The path at which to temporarily store the key pair for the EC2 instance.
KEY_PATH = "/tmp/key.pem"

# The number of seconds to wait between connection attempts to a compilation EC2
# 	instance.
CONNECT_TIMEOUT = 10

# The number of attempts to make to connect to a compilation EC2 instance. The
# 	instance takes some time to start up and CONNECT_TIMEOUT * CONNECT_TRIES
# 	should be greater than this startup time.
CONNECT_TRIES = 9


def compile(debug=False):
    """Compile Cirrus.
    """
    ec2 = boto3.resource("ec2", REGION)

    print("compile: Creating an EC2 key pair.")
    key_name = f"cirrus_compile_key{random.randrange(1_000_000)}"
    response = ec2.meta.client.create_key_pair(KeyName=key_name)
    with open(KEY_PATH, "w+") as f:
        f.write(response["KeyMaterial"])

    print("compile: Creating an EC2 security group.")
    security_group_name = f"cirrus_compile_group{random.randrange(1_000_000)}"
    security_group = ec2.create_security_group(GroupName=security_group_name, Description="bla")
    security_group.authorize_ingress(
        IpProtocol="tcp",
        FromPort=22,
        ToPort=22,
        CidrIp="0.0.0.0/0"
    )

    # Launch EC2 instance.
    print("compile: Launching an EC2 instance.")
    instances = ec2.create_instances(
        BlockDeviceMappings=[
            {
                "DeviceName": "/dev/xvda",
                "Ebs": {
                    "DeleteOnTermination": True,
                    "VolumeSize": DISK_SIZE,
                }
            },
        ],
        KeyName=key_name,
        ImageId=AMI_ID,
        InstanceType=INSTANCE_TYPE,
        MinCount=1,
        MaxCount=1,
        SecurityGroups=[security_group_name]
    )
    instance = instances[0]
    print("compile: Waiting for the EC2 instance to come up.")
    instance.wait_until_running()
    instance.load()  # Reloads metadata about the instance. In particular,
                     #  retreives its public_ip_address.

    print("compile: Compiling Cirrus on the EC2 instance.")
    key = paramiko.RSAKey.from_private_key_file(KEY_PATH)
    para = paramiko.SSHClient()
    para.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for _ in range(CONNECT_TRIES):
        try:
            print("compile: Attempting to connect "
            	  "to the EC2 instance using SSH.")
            para.connect(
            	hostname=instance.public_ip_address,
            	username=SSH_USERNAME,
            	pkey=key,
            	timeout=CONNECT_TIMEOUT
            )
        except socket.timeout:
            pass
        except paramiko.ssh_exception.NoValidConnectionsError:
            time.sleep(CONNECT_TIMEOUT)
            pass
        else:
            break
    for command in COMMANDS.split("\n")[1:-1]:
        print("compile: Running a command:", command)
        _, stdout, stderr = para.exec_command(command)
        stdout.channel.recv_exit_status()
        if debug:
        	print("compile: Printing the stdout of the command.")
        	print(stdout.read())
        	print("compile: Printing the stderr of the command.")
        	print(stderr.read())


if __name__ == "__main__":
    compile()
