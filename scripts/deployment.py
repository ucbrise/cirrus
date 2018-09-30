"""Tools for deploying an instance of Cirrus.
"""
import json
import subprocess
import random
import time
import socket
import tempfile
import datetime

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
#   working directory changes will not persist.
COMMANDS = """
# Install some necessary packages.
yes | sudo yum install git glibc-static openssl-static.x86_64 \
    zlib-static.x86_64 libcurl-devel
yes | sudo yum groupinstall "Development Tools"

# The above installed a recent version of gcc, but an old version of g++.
#   Install a newer version of g++.
yes | sudo yum remove gcc48-c++
yes | sudo yum install gcc72-c++

# The above pulled in an old version of cmake. Install a newer version of cmake
#   by compiling from source.
wget https://cmake.org/files/v3.10/cmake-3.10.0.tar.gz
tar -xvzf cmake-3.10.0.tar.gz
cd cmake-3.10.0; ./bootstrap
cd cmake-3.10.0; make
cd cmake-3.10.0; sudo make install

# Finally, clone Cirrus and compile it.
git clone https://github.com/jcarreira/cirrus
cd cirrus; ./bootstrap.sh
cd cirrus; make -j 10
"""

# The names of Cirrus' executables.
EXECUTABLES = ("parameter_server", "ps_test", "csv_to_libsvm")

# The number of seconds to wait between connection attempts to a compilation EC2
#   instance.
CONNECT_TIMEOUT = 10

# The number of attempts to make to connect to a compilation EC2 instance. The
#   instance takes some time to start up and CONNECT_TIMEOUT * CONNECT_TRIES
#   should be greater than this startup time.
CONNECT_TRIES = 9


def compile(debug=False):
    """Compile Cirrus.

    Creates an EC2 key pair, security group, and instance. Uses the instance to
        compile Cirrus, then downloads the compiled executables.

    Since this function interacts with AWS, it requires that AWS credentials be
        recorded in the filesystem in the manner required by boto3.

    Args:
        debug (bool): Whether to print the stdout and stderr of the compilation
            commands.

    Returns:
        dict[str, file]: The compiled executables, as local temporary files.
    """
    error = None
    key_pair_created = False
    security_group = None
    instance = None

    try:
        now = datetime.datetime.now()
        resource_name = now.strftime("cirrus_%a_%b_%w_%H_%M_%S_%p")

        ec2 = boto3.resource("ec2", REGION)

        print("compile: Creating an EC2 key pair.")
        response = ec2.meta.client.create_key_pair(KeyName=resource_name)
        key_pair_created = True
        key_file = tempfile.TemporaryFile("w+")
        key_file.write(response["KeyMaterial"])
        key_file.seek(0)

        print("compile: Creating an EC2 security group.")
        security_group = ec2.create_security_group(
            GroupName=resource_name, Description="bla")
        # Allow SSH access from anywhere so that we can control the instance.
        security_group.authorize_ingress(
            IpProtocol="tcp",
            FromPort=22,
            ToPort=22,
            CidrIp="0.0.0.0/0"
        )

        # Launch EC2 instance.
        print("compile: Launching an EC2 instance.")
        # The EC2 instance will be created with an EBS volume that gets deleted
        #   automatically when the instance is terminated.
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
            KeyName=resource_name,
            ImageId=AMI_ID,
            InstanceType=INSTANCE_TYPE,
            MinCount=1,
            MaxCount=1,
            SecurityGroups=[resource_name]
        )
        instance = instances[0]
        print("compile: Waiting for the EC2 instance to come up.")
        instance.wait_until_running()
        # Reloads metadata about the instance. In particular, retreives its
        #   public_ip_address.
        instance.load()

        print("compile: Compiling Cirrus on the EC2 instance.")
        key = paramiko.RSAKey.from_private_key(key_file)
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
        # There is a leading and a trailing blank line in the command list.
        for command in COMMANDS.split("\n")[1:-1]:
            print("compile: Running a command:", command)
            _, stdout, stderr = para.exec_command(command)
            # exec_command is asynchronous. This waits for completion.
            stdout.channel.recv_exit_status()
            if debug:
                print("compile: Printing the stdout of the command.")
                print(stdout.read())
                print("compile: Printing the stderr of the command.")
                print(stderr.read())

        print("compile: Downloading the executables from the EC2 instance.")
        sftp = para.open_sftp()
        local_executables = {}
        for executable in EXECUTABLES:
            print("compile: Download an executable:", executable)
            remote_path = f"/home/{SSH_USERNAME}/cirrus/src/{executable}"
            local_executables[executable] = tempfile.TemporaryFile()
            sftp.getfo(remote_path, local_executables[executable])
            local_executables[executable].seek(0)
        sftp.close()
    except (Exception, KeyboardInterrupt, SystemExit) as e:
        print("compile: An error occured.")
        error = e

    try:
        print("compile: Deleting EC2 resources.")
        if instance is not None:
            instance.terminate()
            print("compile: Waiting for the EC2 instance to terminate.")
            instance.wait_until_terminated()
        if security_group is not None:
            security_group.delete()
        if key_pair_created:
            ec2.KeyPair(resource_name).delete()
    except (Exception, KeyboardInterrupt, SystemExit) as e:
        print("compile: An error occured while attempting to delete EC2 "
              "resources. You must delete them yourself. There may be a key "
              "pair, a security group, and an instance.")
        if error is None:
            error = e

    if error is not None:
        raise error

    return local_executables


if __name__ == "__main__":
    for name, file in compile().items():
        print(name, len(file.read()))
