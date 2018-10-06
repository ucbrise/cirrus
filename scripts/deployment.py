"""Tools for deploying an instance of Cirrus.
"""
import json
import subprocess
import time
import socket
import tempfile
import datetime
import traceback
import zipfile
import io
import configparser
import os

import boto3
import paramiko
import tqdm

# Specifications for a compilation EC2 instance.
COMPL_REGION = "us-west-1"
COMPL_DISK_SIZE = 32  # GB
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

# A name for the worker lambda.
LAMBDA_NAME = "cirrus_worker"

# The Lambda runtime for the worker lambda.
LAMBDA_RUNTIME = "python3.6"

# The filename under which to save the code of the worker lambda. This file will
#   exist in the archive uploaded to Lambda.
LAMBDA_CODE_FILENAME = "main.py"

# The fully-qualified identifier for the handler function in the worker
#   lambda's code.
LAMBDA_HANDLER = "main.run"

# The maximum execution time to give to the worker lambda, in seconds. Capped by
#   AWS at 5 minutes.
LAMBDA_TIMEOUT = 5 * 60

# The amount of memory (and in proportion, CPU/network) to give to the worker
#   lambda, in megabytes.
LAMBDA_SIZE = 3_008

# The number of reserved concurrent executions to allocate to the worker lambda.
#   Depletes some of the AWS account's reserved concurrent executions. 100
#   concurrent executions must be held in reserve, not assigned to any lambda.
LAMBDA_CONCURRENCY = 900

# The compression level to use when uploading code to the worker lambda.
LAMBDA_CODE_COMPRESSION = zipfile.ZIP_DEFLATED

# The Amazon resource name of the policy to attach to the role of the worker
#   lambda.
LAMBDA_ROLE_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

# The code of the worker lambda.
LAMBDA_CODE = """
import json
import os
import subprocess

CONFIG_PATH = "/tmp/config.cfg"


def run(event, context):
    executable_path = os.path.join(os.environ["LAMBDA_TASK_ROOT"],
                                   "parameter_server")

    with open(CONFIG_PATH, "w+") as f:
        f.write(event["config"])

    try:
        output = subprocess.check_output([
                executable_path,
                "--config", CONFIG_PATH,
                "--nworkers", str(event["num_workers"]),
                "--rank", str(3),
                "--ps_ip", event["ps_ip"],
                "--ps_port", str(event["ps_port"])
            ], stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as e:
        return {
            "statusCode": 500,
            "body": json.dumps("The worker errored! The stdout/stderr was as "
                               "follows.\\n" + e.output.decode("utf-8"))
        }
    return {
        "statusCode": 200,
        "body": json.dumps("The worker ran successfully. The stdout/stderr was "
                           "as follows.\\n" + output.decode("utf-8"))
    }
"""

# The S3 region in which datasets are stored. Follows hardcoded value in
#   /src/S3Client.cpp.
DATASET_REGION = "us-west-2"


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
            local_executables[executable] = tempfile.NamedTemporaryFile()
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
            key_file = tempfile.NamedTemporaryFile("w+")
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
                    ssh.get_transport().window_size = 2147483647
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
                            ssh_username=COMPL_SSH_USERNAME, return_ssh=False):
    """Launch a parameter server.

    Call `delete_parameter_server` with the returned IP address in order to
        delete the parameter server and stop incurring charges.

    Args:
        executable (file): The Cirrus "parameter_server" excecutable to make
            available on the server.
        region (str): The AWS region to create the instance in.
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
    sftp.chmod(remote_path, 0o777)
    sftp.close()
    if return_ssh:
        return ssh
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


def create_lambda_function(executable, region="us-west-1"):
    """Create a worker lambda function.

    The function will be called `LAMBDA_NAME`.

    Args:
        executable (file): The Cirrus "parameter_server" excecutable to run in
            the function.
        region (str): The AWS region to create the function in.
    """
    lamb = boto3.client("lambda", region)

    # Create an in-memory ZIP file containing the code and executable.
    print("create_lambda_function: Creating the code ZIP file.")
    upload_data = io.BytesIO()
    ctx = zipfile.ZipFile(upload_data, "w", LAMBDA_CODE_COMPRESSION)
    with ctx as upload_zipfile:
        print(f"create_lambda_function: Adding {LAMBDA_CODE_FILENAME} to the "
              "code ZIP file.")
        info = zipfile.ZipInfo(LAMBDA_CODE_FILENAME)
        info.external_attr = 0o777 << 16
        upload_zipfile.writestr(info, LAMBDA_CODE)
        print("create_lambda_function: Adding parameter_server to the "
              "code ZIP file.")
        info = zipfile.ZipInfo("parameter_server")
        info.external_attr = 0o777 << 16
        executable.seek(0)
        upload_zipfile.writestr(info, executable.read())

    # If a previous version of the function already exists, delete it.
    print("create_lambda_function: Deleting any previous function version.")
    try:
        lamb.delete_function(FunctionName=LAMBDA_NAME)
    except Exception:
        pass

    # If a previous version of the IAM role already exists, delete it.
    print("create_lambda_function: Deleting any previous IAM role version.")
    iam = boto3.resource("iam", region)
    try:
        role = iam.Role(LAMBDA_NAME)
        role.detach_policy(PolicyArn=LAMBDA_ROLE_POLICY_ARN)
        role.delete()
    except Exception:
        pass

    # Create an IAM role for the function which allows it to read S3.
    print("create_lambda_function: Creating IAM role for function.")
    role = iam.create_role(
        RoleName=LAMBDA_NAME,
        AssumeRolePolicyDocument=\
        """{
              "Version": "2012-10-17",
              "Statement": [
                {
                  "Effect": "Allow",
                  "Principal": {
                    "Service": "lambda.amazonaws.com"
                  },
                  "Action": "sts:AssumeRole"
                }
              ]
        }"""
    )
    role.attach_policy(PolicyArn=LAMBDA_ROLE_POLICY_ARN)
    print("create_lambda_function: Waiting for IAM role changes to propogate.")
    time.sleep(20)

    # Create the function, uploading the ZIP file.
    print("create_lambda_function: Uploading code ZIP file and creating "
          "function.")
    lamb.create_function(
        FunctionName=LAMBDA_NAME,
        Runtime=LAMBDA_RUNTIME,
        Role=role.arn,
        Handler=LAMBDA_HANDLER,
        Code={"ZipFile": upload_data.getvalue()},
        Timeout=LAMBDA_TIMEOUT,
        MemorySize=LAMBDA_SIZE
    )

    # Allocate reserved concurrent excecutions to the function.
    print("create_lambda_function: Allocating reserved concurrent executions "
          "to the function.")
    lamb.put_function_concurrency(
        FunctionName=LAMBDA_NAME,
        ReservedConcurrentExecutions=LAMBDA_CONCURRENCY
    )


def upload_dataset(executable, config_path):
    """Upload a dataset to S3.

    Creates an S3 bucket to hold the dataset.

    Args:
        executable (file): The "parameter_server" executable to use.
        config_path (str): The path to the configuration file to use.
    """
    print("upload_dataset: Reading configuration.")
    config = configparser.ConfigParser()
    config.read_string("[main]\n" + open(config_path).read())
    assert "s3_bucket" in config["main"], \
        "The configuration must specify an s3_bucket."
    assert "load_input_path" in config["main"], \
        "The configuration must specify a load_input_path."

    print("upload_dataset: Deleting any old version of the bucket.")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(config["main"]["s3_bucket"])
    try:
        bucket.delete()
    except Exception:
        pass

    print("upload_dataset: Creating the bucket.")
    create_config = {"LocationConstraint": DATASET_REGION}
    bucket.create(CreateBucketConfiguration=create_config)

    print("upload_dataset: Launching a server to do the uploading.")
    ssh = launch_parameter_server(executable, return_ssh=True)

    print("upload_dataset: Uploading the data to the server.")
    sftp = ssh.open_sftp()
    with tqdm.tqdm() as pbar:
        def make_put_callback():
            prev = 0
            def put_callback(curr, tot):
                pbar.total = tot
                nonlocal prev
                pbar.update(curr - prev)
                prev = curr
            return put_callback
        sftp.put(config["main"]["load_input_path"], f"/home/{COMPL_SSH_USERNAME}/data", callback=make_put_callback())
    config["main"]["load_input_path"] = f"/home/{COMPL_SSH_USERNAME}/data"
    f = io.StringIO()
    config.write(f)
    f.seek(0)
    f.seek(f.getvalue().find("\n") + 1)
    sftp.putfo(f, f"/home/{COMPL_SSH_USERNAME}/config")
    sftp.close()
    
    print("upload_dataset: Uploading the dataset.")
    _, stdout, stderr = ssh.exec_command(" ".join([
        f"/home/{COMPL_SSH_USERNAME}/parameter_server",
        f"--config /home/{COMPL_SSH_USERNAME}/config",
        "--nworkers 4",
        "--rank 0"
    ]))
    # exec_command is asynchronous. This waits for completion.
    stdout.channel.recv_exit_status()
    print(stdout.read())
    print(stderr.read())
    

if __name__ == "__main__":
    with open("../../cirrus_ubuntu_compiled/src/parameter_server", "rb") as f:
        upload_dataset(f, "../configs/criteo_kaggle.cfg")
