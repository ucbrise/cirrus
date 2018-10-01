"""Tools for deploying an instance of Cirrus.
"""
import json
import subprocess
import random
import time
import socket
import tempfile
import datetime
import traceback

import boto3
from botocore.exceptions import ClientError
import paramiko

# Specifications for a compilation EC2 instance.
COMPL_REGION = "us-west-1"
COMPL_DISK_SIZE = 16  # GB
COMPL_AMI_ID = "ami-3a674d5a"  # This is amzn-ami-hvm-2017.03.1.20170812-x86_64-gp2,
                         #  which is recommended by AWS as of Sep 27, 2018 for
                         #  compiling executables for Lambda.
COMPL_INSTANCE_TYPE = "t3.xlarge"
COMPL_SSH_USERNAME = "ec2-user"

# A series of commands that will result in ~/cirrus/src containing Cirrus'
#   compiled executables. Note that each command is issued separately, so
#   working directory changes will not persist.
COMPL_COMMANDS = """
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


def compile(debug=False):
    """Compile Cirrus.

    Creates an EC2 key pair, security group, and instance. Uses the instance to
        compile Cirrus, then downloads the compiled executables.

    Args:
        debug (bool): Whether to print the stdout and stderr of the compilation
            commands.

    Returns:
        dict[str, file]: The compiled executables, as local temporary files.
    """
    ctx = EC2InstanceContextManager(COMPL_REGION, COMPL_DISK_SIZE, COMPL_AMI_ID,
        COMPL_INSTANCE_TYPE, COMPL_SSH_USERNAME, "compile")

    with ctx as ssh:
        # There is a leading and a trailing blank line in the command list.
        for command in COMPL_COMMANDS.split("\n")[1:-1]:
            print("compile: Running a command:", command)
            _, stdout, stderr = ssh.exec_command(command)
            # exec_command is asynchronous. This waits for completion.
            stdout.channel.recv_exit_status()
            if debug:
                print("compile: Printing the stdout of the command.")
                print(stdout.read())
                print("compile: Printing the stderr of the command.")
                print(stderr.read())

        print("compile: Downloading the executables from the EC2 instance.")
        sftp = ssh.open_sftp()
        local_executables = {}
        for executable in EXECUTABLES:
            print("compile: Downloading an executable:", executable)
            remote_path = f"/home/{COMPL_SSH_USERNAME}/cirrus/src/{executable}"
            local_executables[executable] = tempfile.TemporaryFile()
            sftp.getfo(remote_path, local_executables[executable])
            local_executables[executable].seek(0)
        sftp.close()

        return local_executables


class EC2InstanceContextManager:
    """A context manager that creates an EC2 instance and connects to it over
        SSH. Since this class interacts with AWS, it requires that AWS
        credentials be recorded in the filesystem in the manner required by
        boto3.
    """

    # The number of seconds to wait between SSH connection attempts.
    _CONNECT_TIMEOUT = 10

    # The number of SSH connection attempts to make. Instances take some time to
    #   start up; _CONNECT_TIMEOUT * _CONNECT_TRIES should be greater than this
    #   startup time.
    _CONNECT_TRIES = 9

    def __init__(self, region, disk_size, ami_id, instance_type, ssh_username,
                 log_prefix):
        """Create an EC2 instance context manager.

        Args:
            region (str): The region to create the instance in.
            disk_size (int): The amount of disk space to give the instance, in
                gigabytes.
            ami_id (str): The ID of the AMI to create the instance from.
            instance_type (str): The type of instance to create.
            ssh_username (str): The username to use when accessing the instance
                via SSH.
            log_prefix (str): A string to prefix log messages with when printing
                them out.
        """
        self._region = region
        self._disk_size = disk_size
        self._ami_id = ami_id
        self._instance_type = instance_type
        self._ssh_username = ssh_username
        self._log_prefix = log_prefix
        now = datetime.datetime.now()
        self._resource_name = now.strftime("cirrus_%a_%b_%w_%H_%M_%S_%p")
        self._key_pair_created = False
        self._security_group = None
        self._instance = None

    def __enter__(self):
        """Create the instance and connect to it via SSH.

        Returns:
            paramiko.client.SSHClient: An SSH connection to the instance.
        """
        try:
            self._ec2 = boto3.resource("ec2", self._region)

            print(f"{self._log_prefix}: Creating an EC2 key pair.")
            response = self._ec2.meta.client.create_key_pair(
                KeyName=self._resource_name)
            self._key_pair_created = True
            key_file = tempfile.TemporaryFile("w+")
            key_file.write(response["KeyMaterial"])
            key_file.seek(0)

            print(f"{self._log_prefix}: Creating an EC2 security group.")
            self._security_group = self._ec2.create_security_group(
                GroupName=self._resource_name, Description="bla")
            # Allow SSH access from anywhere so that we can control the
            #   instance.
            self._security_group.authorize_ingress(
                IpProtocol="tcp",
                FromPort=22,
                ToPort=22,
                CidrIp="0.0.0.0/0"
            )

            # Launch EC2 instance.
            print(f"{self._log_prefix}: Launching an EC2 instance.")
            # The EC2 instance will be created with an EBS volume that gets
            #   deleted automatically when the instance is terminated.
            instances = self._ec2.create_instances(
                BlockDeviceMappings=[
                    {
                        "DeviceName": "/dev/xvda",
                        "Ebs": {
                            "DeleteOnTermination": True,
                            "VolumeSize": self._disk_size,
                        }
                    },
                ],
                KeyName=self._resource_name,
                ImageId=self._ami_id,
                InstanceType=self._instance_type,
                MinCount=1,
                MaxCount=1,
                SecurityGroups=[self._resource_name]
            )
            self._instance = instances[0]
            print(f"{self._log_prefix}: Waiting for the EC2 instance to come "
                   "up.")
            self._instance.wait_until_running()
            # Reloads metadata about the instance. In particular, retreives its
            #   public_ip_address.
            self._instance.load()

            key = paramiko.RSAKey.from_private_key(key_file)
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            success = False
            for _ in range(self._CONNECT_TRIES):
                try:
                    print(f"{self._log_prefix}: Attempting to connect "
                           "to the EC2 instance using SSH.")
                    ssh.connect(
                        hostname=self._instance.public_ip_address,
                        username=self._ssh_username,
                        pkey=key,
                        timeout=self._CONNECT_TIMEOUT
                    )
                except socket.timeout:
                    pass
                except paramiko.ssh_exception.NoValidConnectionsError:
                    time.sleep(self._CONNECT_TIMEOUT)
                    pass
                else:
                    success = True
                    break
            if success:
                return ssh
            else:
                raise RuntimeError("Failed to connect to the EC2 instance.")
        except (Exception, KeyboardInterrupt, SystemExit) as e:
            print(f"{self._log_prefix}: An error occured.")
            traceback.print_exc(e)
            self.__exit__()
            raise e

    def __exit__(self, *args):
        """Delete the instance and any other AWS resources created for it.
        """
        try:
            if self._instance is not None:
                self._instance.terminate()
                print(f"{self._log_prefix}: Waiting for the EC2 instance to "
                       "terminate.")
                self._instance.wait_until_terminated()
            if self._security_group is not None:
                self._security_group.delete()
            if self._key_pair_created:
                self._ec2.KeyPair(self._resource_name).delete()
        except (Exception, KeyboardInterrupt, SystemExit) as e:
            print(f"{self._log_prefix}: An error occured while attempting to "
                  "delete EC2 resources. You must delete them yourself. There "
                  "may be a key pair, a security group, and an instance.")
            raise e


parameter_servers = {}


def launch_parameter_server(executable, region="us-west-1", disk_size=16,
                            ami_id=COMPL_AMI_ID, instance_type="t3.xlarge",
                            ssh_username=COMPL_SSH_USERNAME):
    """Launch a parameter server.

    Call `delete_parameter_server` with the returned IP address in order to
        delete the parameter server and stop incurring charges.

    Args:
        executable (file): The Cirrus "parameter_server" excecutable to make
            available on the server.
        region (str): The region to create the instance in.
        disk_size (int): The amount of disk space to give the instance, in
            gigabytes.
        instance_type (str): The type of EC2 instance to use.
        ami_id (str): The ID of the AMI to create the instance from.
        instance_type (str): The type of instance to create.

    Returns:
        str: The public IPv4 address of the server.
    """
    ctx = EC2InstanceContextManager(region, disk_size, ami_id, instance_type,
        ssh_username, "launch_parameter_server")
    ssh = ctx.__enter__()
    print("launch_parameter_server: Uploading the parameter server executable "
          "to the EC2 instance.")
    sftp = ssh.open_sftp()
    remote_path = f"/home/{ssh_username}/parameter_server"
    sftp.putfo(executable, remote_path)
    sftp.close()
    ip = ssh.get_transport().getpeername()[0]
    parameter_servers[ip] = ctx
    return ip


def delete_parameter_server(ip):
    """Delete a parameter server.

    Args:
        ip (str): The public IPv4 address of the server.
    """
    parameter_servers[ip].__exit__()
    del parameter_servers[ip]


if __name__ == "__main__":
    executables = compile()
    launch_parameter_server(executables["parameter_server"])

