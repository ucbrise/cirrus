import logging
import time
import socket
import sys
import io
import atexit
import zipfile
import pipes
import json
import threading

import paramiko
import boto3


# A configuration to use for EC2 instances that will be used to build Cirrus.
BUILD_INSTANCE = {
    "region": "us-west-1",
    "disk_size": 32,  # GB
    # This is amzn-ami-hvm-2017.03.1.20170812-x86_64-gp2, which is recommended
    #   by AWS as of Sep 27, 2018 for compiling executables for Lambda.
    "ami_id": "ami-3a674d5a",
    "typ": "t3.xlarge",
    "username": "ec2-user"
}
BUILD_IMAGE_NAME = "cirrus_build_image"
SERVER_IMAGE_NAME = "cirrus_server_image"
EXECUTABLES_PATH = "s3://cirrus-public/executables"
LAMBDA_PACKAGE_PATH = "s3://cirrus-public/lambda_package"
LAMBDA_NAME = "cirrus_lambda"

# The ARN of an IAM policy that allows full access to S3.
S3_FULL_ACCESS_ARN = "arn:aws:iam::aws:policy/AmazonS3FullAccess"

# The ARN of an IAM policy that allows read-only access to S3.
S3_READ_ONLY_ARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

# The ARN of an IAM policy that allows write access to Cloudwatch logs.
CLOUDWATCH_WRITE_ARN = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"

# The estimated delay of IAM's eventual consistency, in seconds.
IAM_CONSISTENCY_DELAY = 20

# A series of commands that sets up the proper build environment.
ENVIRONMENT_COMMANDS = """
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
"""

# A series of commands that results in ~/cirrus/src containing Cirrus'
#   executables.
BUILD_COMMANDS = """
git clone https://github.com/jcarreira/cirrus
cd cirrus; ./bootstrap.sh
cd cirrus; ./configure --enable-static-nss --disable-option-checking --prefix=/home/ec2-user/glibc
cd cirrus; make -j 10
"""

# The filenames of Cirrus' executables.
EXECUTABLES = ("parameter_server", "ps_test", "csv_to_libsvm")

# The level of compression to use when creating the Lambda ZIP package.
LAMBDA_COMPRESSION = zipfile.ZIP_DEFLATED

# The name to give to the file containing the Lambda's handler code.
LAMBDA_HANDLER_FILENAME = "main.py"

# The Lambda's handler code.
LAMBDA_HANDLER = r"""
import json
import os
import subprocess
import sys
import logging
import time

CONFIG_PATH = "/tmp/config.cfg"
EXIT_POLL_INTERVAL = 0.001


def run(event, _):
    log = logging.getLogger("main.run")
    
    log.debug("Starting.")
    
    try:
        executable_path = os.path.join(os.environ["LAMBDA_TASK_ROOT"],
                                       "parameter_server")
        log.debug("Determined executable path to be %s." % executable_path)
    
        with open(CONFIG_PATH, "w+") as config_file:
            config_file.write(event["config"])
        log.debug("Wrote config to %s." % CONFIG_PATH)
    
        command = [
            executable_path,
            "--config", CONFIG_PATH,
            "--nworkers", str(event["num_workers"]),
            "--rank", str(3),
            "--ps_ip", event["ps_ip"],
            "--ps_port", str(event["ps_port"])
        ]
        log.debug("Starting worker.")
        log.debug(" ".join(command))
        process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        for c in iter(lambda: process.stdout.read(1), b''):
            sys.stdout.write(c)
        
        while process.poll() is None:
            time.sleep(EXIT_POLL_INTERVAL)
            
        if process.returncode >= 0:
            msg = "The worker exited with code %d." % process.returncode
        else:
            msg = "The worker died with signal %d." % (-process.returncode)
        log.debug(msg)
        
        if process.returncode != 0:
            raise RuntimeError(msg)
        
        log.debug("Done.")
        
        return {
            "statusCode": 200,
            "body": json.dumps(msg)
        }
    except:
        log.debug("The handler threw an error.")
        raise
"""

# The estimated delay of S3's eventual consistency, in seconds.
S3_CONSISTENCY_DELAY = 20

# The runtime for the worker Lambda.
LAMBDA_RUNTIME = "python3.6"

# The fully-qualified identifier of the handler of the worker Lambda.
LAMBDA_HANDLER_FQID = "main.run"

# The maximum execution time to give the worker Lambda, in seconds. Capped by AWS at 5 minutes.
LAMBDA_TIMEOUT = 5 * 60

# The amount of memory (and in proportion, CPU/network) to give to the worker Lambda, in megabytes.
LAMBDA_SIZE = 3008


log = logging.getLogger("cirrus.automate")
log.debug("automate: Initializing Lambda client.")
# TODO: Pull out region as a configuration value.
lamb = boto3.client("lambda", BUILD_INSTANCE["region"])


class Instance(object):
    """An EC2 instance."""

    # The interval at which to poll for an AMI becoming available, in seconds.
    IMAGE_POLL_INTERVAL = 10

    # The maximum number of times to poll for an AMI becoming available.
    IMAGE_POLL_MAX = (5 * 60) // IMAGE_POLL_INTERVAL

    @staticmethod
    def images_exist(name):
        """Return whether any AMI with a given name, owned by the current user,
            exists.

        Args:
            name (str): The name.

        Returns:
            bool: Whether any exists.
        """
        log = logging.getLogger("cirrus.automate.Instance")

        ec2 = boto3.resource("ec2", BUILD_INSTANCE["region"])

        log.debug("images_exist: Describing images.")
        response = ec2.meta.client.describe_images(
            Filters=[{"Name": "name", "Values": [name]}], Owners=["self"])
        result = len(response["Images"]) > 0

        log.debug("images_exist: Done.")

        return result


    @staticmethod
    def delete_images(name):
        """Delete any AMI with a given name, owned by the current user.

        Args:
            name (str): The name.
        """
        log = logging.getLogger("cirrus.automate.Instance")

        ec2 = boto3.resource("ec2", BUILD_INSTANCE["region"])

        log.debug("delete_images: Describing images.")
        response = ec2.meta.client.describe_images(
            Filters=[{"Name": "name", "Values": [name]}], Owners=["self"])

        for info in response["Images"]:
            image_id = info["ImageId"]
            log.debug("delete_images: Deleting image %s." % image_id)
            ec2.Image(info["ImageId"]).deregister()

        log.debug("delete_images: Done.")

    def __init__(self, name, region, disk_size, typ, username, ami_id=None, ami_name=None):
        """Define an EC2 instance.

        Args:
            name (str): Name for the instance. The same name will be used for
                the key pair and security group that get created.
            region (str): Region for the instance.
            disk_size (int): Disk space for the instance, in GB.
            typ (str): Type for the instance.
            username (str): SSH username for the AMI.
            ami_id (str): ID of the AMI for the instance. If omitted or None, `ami_name` must be provided.
            ami_name (str): Name of the AMI for the instance. Only used if `ami_id` is not provided. The first AMI with
                the name `ami_name` owned by the AWS account is used.
        """
        self._name = name
        self._region = region
        self._disk_size = disk_size
        self._ami_id = ami_id
        self._type = typ
        self._username = username
        self._log = logging.getLogger("cirrus.automate.Instance")

        self._log.debug("__init__: Initializing EC2.")
        self._ec2 = boto3.resource("ec2", self._region)

        if self._ami_id is None:
            self._log.debug("__init__: Resolving AMI name to AMI ID.")
            response = self._ec2.meta.client.describe_images(
                Filters=[{"Name": "name", "Values": [ami_name]}], Owners=["self"])
            if len(response["Images"]) > 0:
                self._ami_id = response["Images"][0]["ImageId"]
            else:
                raise RuntimeError("No AMIs with the given name were found.")

        self._role = None
        self._instance_profile = None
        self._key_pair = None
        self._private_key = None
        self._security_group = None
        self.instance = None
        self._ssh_client = None
        self._sftp_client = None

        self._buffering_commands = False
        self._buffered_commands = []

        self._log.debug("__init__: Done.")


    def start(self):
        """Start the instance.

        When finished, call `cleanup`. `cleanup` will also be registered as an
            `atexit` cleanup function so that it will still be called despite
            any errors.
        """
        atexit.register(self.cleanup)

        self._log.debug("start: Calling _make_instance_profile.")
        self._make_instance_profile()

        self._log.debug("start: Calling _make_key_pair.")
        self._make_key_pair()

        self._log.debug("start: Calling _make_security_group.")
        self._make_security_group()

        self._log.debug("start: Calling _start_and_wait.")
        self._start_and_wait()

        self._log.debug("start: Done.")


    def public_ip(self):
        return self.instance.public_ip_address


    def private_ip(self):
        return self.instance.private_ip_address


    def private_key(self):
        return self._private_key


    def run_command(self, command):
        """Run a command on this instance.

        Args:
            command (str): The command to run.

        Returns:
            tuple[int, bytes, bytes]: The exit code, stdout, and stderr,
                respectively, of the process.
        """
        if self._buffering_commands:
            self._buffered_commands.append(command)
            return 0, "", ""

        if self._ssh_client is None:
            self._log.debug("run_command: Calling _connect_ssh.")
            self._connect_ssh()

        self._log.debug("run_command: Running `%s`." % command)
        _, stdout, stderr = self._ssh_client.exec_command(command)

        self._log.debug("run_command: Waiting for completion.")
        # exec_command is asynchronous. This waits for completion.
        status = stdout.channel.recv_exit_status()
        self._log.debug("run_command: Exit code was %d." % status)

        self._log.debug("run_command: Fetching stdout and stderr.")
        stdout, stderr = stdout.read(), stderr.read()
        self._log.debug("run_command: stdout had length %d." % len(stdout))
        self._log.debug("run_command: stderr had length %d." % len(stderr))

        self._log.debug("run_command: Done.")
        return status, stdout, stderr


    def buffer_commands(self, flag):
        if flag == False and self._buffering_commands == True:
            concat_command = "\n".join(self._buffered_commands)
            self._buffered_commands = []
            self._buffering_commands = False
            return self.run_command(concat_command)
        else:
            if flag == True and self._buffering_commands == False:
                self._buffering_commands = True
            return 0, "", ""


    def download_s3(self, src, dest):
        """Download a file from S3 to this instance.

        Args:
            src (str): A path to a file on S3.
            dest (str): The path at which to save the file on this instance.
                If relative, then relative to the home folder of this instance's
                SSH user.
        """
        assert src.startswith("s3://")
        assert not dest.startswith("s3://")

        self.run_command(" ".join((
            "aws",
            "s3",
            "cp",
            src,
            dest
        )))


    def upload_s3(self, src, dest):
        """Upload a file from this instance to S3.

        Args:
            src (str): A path to a file on this instance. If relative, then
                relative to the home folder of this instance's SSH user.
            dest (str): A path on S3 to upload to.
        """
        assert not src.startswith("s3://")
        assert dest.startswith("s3://")

        self.run_command(" ".join((
            "aws",
            "s3",
            "cp",
            src,
            dest
        )))


    def upload(self, content, dest):
        if self._sftp_client is None:
            self._connect_sftp()
        fo = io.StringIO(content)
        self._sftp_client.putfo(fo, dest)


    def save_image(self, name):
        """Create an AMI from the current state of this instance.

        Args:
            name (str): The name to give the AMI.
        """
        self._log.debug("save_image: Starting image creation.")
        image = self.instance.create_image(Name=name)

        self._log.debug("save_image: Waiting for image creation.")
        image.wait_until_exists()
        for i in range(self.IMAGE_POLL_MAX):
            self._log.debug("make_build_image: Doing poll #%d out of "
                      "%d." % (i+1, self.IMAGE_POLL_MAX))
            image.reload()
            if image.state == "available":
                break
            time.sleep(self.IMAGE_POLL_INTERVAL)
        else:
            raise RuntimeError("AMI did not become available within time "
                               "constraints.")

        self._log.debug("save_image: Done.")


    def cleanup(self):
        """Terminate the instance and clean up all associated resources.
        """
        try:
            if self._sftp_client is not None:
                self._log.debug("cleanup: Closing SFTP client.")
                self._sftp_client.close()
                self._sftp_client = None
            if self._ssh_client is not None:
                self._log.debug("cleanup: Closing SSH client.")
                self._ssh_client.close()
                self._ssh_client = None
            if self.instance is not None:
                self._log.debug("cleanup: Terminating instance.")
                self.instance.terminate()
                self._log.debug("cleanup: Waiting for instance to terminate.")
                self.instance.wait_until_terminated()
                self.instance = None
            if self._security_group is not None:
                self._log.debug("cleanup: Deleting security group.")
                self._security_group.delete()
                self._security_group = None
            if self._key_pair is not None:
                self._log.debug("cleanup: Deleting key pair.")
                self._key_pair.delete()
                self._key_pair = None
            if self._instance_profile is not None:
                self._log.debug("cleanup: Deleting instance profile.")
                self._instance_profile.remove_role(RoleName=self._role.name)
                self._instance_profile.delete()
                self._instance_profile = None
            if self._role is not None:
                self._log.debug("cleanup: Deleting role.")
                self._role.detach_policy(PolicyArn=S3_FULL_ACCESS_ARN)
                self._role.delete()
                self._role = None
            self._log.debug("cleanup: Done.")
        except:
            MESSAGE = "An error occured during cleanup. Some EC2 resources " \
                  "may remain. Delete them manually."
            print("=" * len(MESSAGE))
            print(MESSAGE)
            print("=" * len(MESSAGE))
            raise sys.exc_info()[1]


    def _make_instance_profile(self):
        self._log.debug("_make_instance_profile: Initializing IAM.")
        iam = boto3.resource("iam", self._region)

        self._log.debug("_make_instance_profile: Creating role.")
        self._role = iam.create_role(
            RoleName=self._name,
            AssumeRolePolicyDocument="""{
                  "Version": "2012-10-17",
                  "Statement": [
                    {
                      "Effect": "Allow",
                      "Principal": {
                        "Service": "ec2.amazonaws.com"
                      },
                      "Action": "sts:AssumeRole"
                    }
                  ]
            }"""
        )

        self._log.debug("_make_instance_profile: Attaching policy to role.")
        self._role.attach_policy(PolicyArn=S3_FULL_ACCESS_ARN)

        self._log.debug("_make_instance_profile: Creating instance profile.")
        self._instance_profile = iam.create_instance_profile(
            InstanceProfileName=self._name
        )

        self._log.debug("_make_instance_profile: Adding role to instance " \
                        "profile.")
        self._instance_profile.add_role(RoleName=self._role.name)

        self._log.debug("_make_instance_profile: Waiting for changes to take " \
                        "effect.")
        # IAM is eventually consistent, so we need to wait for our changes to be
        #   reflected. The delay distribution is heavy-tailed, so this might
        #   still error, rarely. The right way is to retry at an interval.
        time.sleep(IAM_CONSISTENCY_DELAY)


        self._log.debug("_make_instance_profile: Done.")


    def _make_key_pair(self):
        self._log.debug("_make_key_pair: Creating new key pair.")
        response = self._ec2.meta.client.create_key_pair(
                KeyName=self._name)

        self._log.debug("_make_key_pair: Saving private key.")
        self._private_key = response["KeyMaterial"]

        self._log.debug("_make_key_pair: Fetching key metadata.")
        self._key_pair = self._ec2.KeyPair(self._name)
        self._key_pair.load()

        self._log.debug("_make_key_pair: Done.")


    def _make_security_group(self):
        self._log.debug("_make_security_group: Creating new security group.")
        self._security_group = self._ec2.create_security_group(
            GroupName=self._name, Description="Auto-generated by Cirrus. "\
                "Allows TCP traffic in on port 22.")

        self._log.debug("_make_security_group: Configuring security group.")
        # Allow access from anywhere so that we can communicate with programs
        #   running on the instance.
        self._security_group.authorize_ingress(
            IpProtocol="-1",  # -1 means any protocols (and implies any port).
            CidrIp="0.0.0.0/0"
        )

        self._log.debug("_make_security_group: Done.")


    def _start_and_wait(self):
        self._log.debug("_start_and_wait: Starting a new instance.")
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
            KeyName=self._key_pair.name,
            ImageId=self._ami_id,
            InstanceType=self._type,
            MinCount=1,
            MaxCount=1,
            SecurityGroups=[self._security_group.group_name],
            IamInstanceProfile={"Name": self._instance_profile.name}
        )
        self.instance = instances[0]

        self._log.debug("_start_and_wait: Waiting for instance to enter " \
                        "running state.")
        self.instance.wait_until_running()

        self._log.debug("_start_and_wait: Fetching instance metadata.")
        # Reloads metadata about the instance. In particular, retreives its
        #   public_ip_address.
        self.instance.load()

        self._log.debug("_start_and_wait: Done.")


    def _connect_ssh(self, timeout=10, attempts=10):
        self._log.debug("_connect_ssh: Configuring.")
        key = paramiko.RSAKey.from_private_key(io.StringIO(unicode(self._private_key, "utf-8")))
        self._ssh_client = paramiko.SSHClient()
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        for i in range(attempts):
            try:
                self._log.debug("_connect_ssh: Making connection attempt " \
                                "#%d out of %d." % (i+1, attempts))
                self._ssh_client.connect(
                    hostname=self.instance.public_ip_address,
                    username=self._username,
                    pkey=key,
                    timeout=timeout
                )
                self._ssh_client.get_transport().window_size = 2147483647
            except socket.timeout:
                self._log.debug("_connect_ssh: Connection attempt timed out " \
                                "after %ds." % timeout)
                pass
            except paramiko.ssh_exception.NoValidConnectionsError:
                self._log.debug("_connect_ssh: Connection attempt failed. " \
                                "Sleeping for %ds." % timeout)
                time.sleep(timeout)
                pass
            else:
                break
        else:
            pass  # FIXME


    def _connect_sftp(self):
        if self._ssh_client is None:
            self._connect_ssh()
        self._sftp_client = self._ssh_client.open_sftp()


class ParameterServer(object):
    def __init__(self, instance, ps_port, error_port, num_workers):
        self._instance = instance
        self._ps_port = ps_port
        self._error_port = error_port
        self._num_workers = num_workers

        self._log = logging.getLogger("cirrus.automate.ParameterServer")
        self._ps_pid = None
        self._error_pid = None


    def public_ip(self):
        return self._instance.public_ip()


    def private_ip(self):
        return self._instance.private_ip()


    def ps_port(self):
        return self._ps_port


    def error_port(self):
        return self._error_port


    def start(self, config):
        self._log.debug("start: Uploading configuration.")
        config_filename = "config_%d.txt" % self._ps_port
        self._instance.run_command(
            "echo %s > %s" % (pipes.quote(config), config_filename))

        self._log.debug("start: Starting parameter server.")
        ps_start_command = " ".join((
            "nohup",
            "./parameter_server",
            "--config", config_filename,
            "--nworkers", str(self._num_workers),
            "--rank", "1",
            "--ps_port", str(self._ps_port),
            "&>", "ps_out_%d" % self._ps_port,
            "&"
        ))
        status, _, stderr = self._instance.run_command(ps_start_command)
        if status != 0:
            print("An error occurred while starting the parameter server."
                  " The exit code was %d and the stderr was:" % status)
            print(stderr)
            raise RuntimeError("An error occurred while starting the parameter"
                               " server.")


        self._log.debug("start: Retreiving parameter server PID.")
        status, _, stderr = self._instance.run_command(
            "echo $! > ps_%d.pid" % self._ps_port)
        if status != 0:
            print("An error occurred while getting the PID of the parameter"
                  " server. The exit code was %d and the stderr was:" % status)
            print(stderr)
            raise RuntimeError("An error occurred while getting the PID of the"
                               " parameter server.")


        self._log.debug("start: Starting error task.")
        error_start_command = " ".join((
            "nohup",
            "./parameter_server",
            "--config", config_filename,
            "--nworkers", str(self._num_workers),
            "--rank 2",
            "--ps_ip", self._instance.private_ip(),
            "--ps_port", str(self.ps_port()),
            "&> error_out_%d" % self.ps_port(),
            "&"
        ))
        status, _, stderr = self._instance.run_command(error_start_command)
        if status != 0:
            print("An error occurred while starting the error task."
                  " The exit code was %d and the stderr was:" % status)
            print(stderr)
            raise RuntimeError("An error occurred while starting the error"
                               " task.")


        self._log.debug("start: Retreiving error task PID.")
        status, _, stderr = self._instance.run_command(
            "echo $! > error_%d.pid" % self.ps_port())
        if status != 0:
            print("An error occurred while getting the PID of the error task. "
                  " The exit code was %d and the stderr was:" % status)
            print(stderr)
            raise RuntimeError("An error occurred while getting the PID of the"
                               " error task.")


    def stop(self):
        for task in ("error", "ps"):
            kill_command = "kill -9 $(cat %s_%d.pid)" % (task, self.ps_port())
            _, _, _ = self._instance.run_command(kill_command)
            # TODO: Here we should probably wait for the process to die and
            #   raise an error if it doesn't in a certain amount of time.


    def ps_output(self):
        command = "cat ps_out_%d" % self.ps_port()
        status, stdout, stderr = self._instance.run_command(command)
        if status != 0:
            print("An error occurred while getting the output of the parameter "
                  "server. The exit code was %d and the stderr was:" % status)
            print(stderr)
            raise RuntimeError("An error occurred while getting the output of "
                               " the parameter server.")
        return stdout


    def error_output(self):
        command = "cat error_out_%d" % self.ps_port()
        status, stdout, stderr = self._instance.run_command(command)
        if status != 0:
            print("An error occurred while getting the output of the error "
                  "task. The exit code was %d and the stderr was:" % status)
            print(stderr)
            raise RuntimeError("An error occurred while getting the output of "
                               " the error task.")
        return stdout


def make_build_image(name, replace=False):
    """Make an AMI sutiable for compiling Cirrus on.

    Args:
        name (str): The name to give the AMI.
        region (str): The region for the AMI.
        replace (bool): Whether to replace any existing AMI with the same name.
            If False or omitted and an AMI with the same name exists, nothing
            will be done.
    """
    log = logging.getLogger("cirrus.automate.make_build_image")

    log.debug("make_build_image: Initializing EC2.")
    ec2 = boto3.resource("ec2", BUILD_INSTANCE["region"])

    log.debug("make_build_image: Checking for already-existent images.")
    if replace:
        Instance.delete_images(name)
    else:
        if Instance.images_exist(name):
            log.debug("make_build_image: Done.")
            return

    log.debug("make_build_image: Launching an instance.")
    instance = Instance("make_build_image", **BUILD_INSTANCE)
    instance.start()

    for command in ENVIRONMENT_COMMANDS.split("\n")[1:-1]:
        log.debug("make_build_image: Running a build command.")
        status, stdout, stderr = instance.run_command(command)
        if status != 0:
            MESSAGE = "A build command errored."
            print("=" * len(MESSAGE))
            print(MESSAGE)
            print("=" * len(MESSAGE))
            print("command:", command)
            print()
            print("stdout:", stdout)
            print()
            print("stderr:", stderr)
            print()
            raise RuntimeError("A build command errored.")

    instance.save_image(name)

    log.debug("make_build_image: Cleaning up instance.")
    instance.cleanup()

    log.debug("make_build_image: Done.")


def make_executables(path, instance):
    """Compile Cirrus and publish its executables.

    Args:
        path (str): A S3 path to a "directory" in which to publish the
            executables.
        instance (EC2Instance): The instance on which to compile. Should use an
            AMI produced by `make_build_image`.
    """
    log = logging.getLogger("cirrus.automate.make_executables")

    log.debug("make_executables: Running build commands.")
    for command in BUILD_COMMANDS.split("\n")[1:-1]:
        status, stdout, stderr = instance.run_command(command)
        if status != 0:
            MESSAGE = "A build command had nonzero exit status."
            print("="*10, MESSAGE, "="*10)
            print("command:", command, "\n")
            print("stdout:", stdout, "\n")
            print("stderr:", stderr, "\n")
            raise RuntimeError(MESSAGE)

    log.debug("make_executables:  Publishing executables.")
    for executable in EXECUTABLES:
        instance.upload_s3("~/cirrus/src/%s" % executable, path + "/" + executable)

    log.debug("make_executables:  Done.")


def make_lambda_package(path, executables_path):
    """Make and publish the ZIP package for Cirrus' Lambda function.

    Args:
        path (str): An S3 path at which to publish the package.
        executables_path (str): An S3 path to a "directory" from which to get
            Cirrus' executables.
    """
    assert path.startswith("s3://")
    assert executables_path.startswith("s3://")

    log = logging.getLogger("cirrus.automate.make_lambda_package")

    log.debug("make_lambda_package: Initializing ZIP file.")
    file = io.BytesIO()
    with zipfile.ZipFile(file, "w", LAMBDA_COMPRESSION) as zip:
        log.debug("make_lambda_package: Writing handler.")
        info = zipfile.ZipInfo(LAMBDA_HANDLER_FILENAME)
        info.external_attr = 0o777 << 16  # Allow, in particular, execute permissions.
        zip.writestr(info, LAMBDA_HANDLER)

        log.debug("make_lambda_package: Initializing S3.")
        s3_client = boto3.client("s3")
        executable = io.BytesIO()

        log.debug("make_lambda_package: Downloading executable.")
        executables_path += "/parameter_server"
        bucket, key = _split_s3_url(executables_path)
        s3_client.download_fileobj(bucket, key, executable)

        log.debug("make_lambda_package: Writing executable.")
        info = zipfile.ZipInfo("parameter_server")
        info.external_attr = 0o777 << 16  # Allow, in particular, execute permissions.
        executable.seek(0)
        zip.writestr(info, executable.read())

    log.debug("make_lambda_package: Uploading package.")
    file.seek(0)
    bucket, key = _split_s3_url(path)
    s3_client.upload_fileobj(file, bucket, key)

    log.debug("make_lambda_package: Waiting for changes to take effect.")
    # Waits for S3's eventual consistency to catch up. Ideally, something more sophisticated would be used since the
    #   delay distribution is heavy-tailed. But this should in most cases ensure the package is visible on S3 upon
    #   return.
    time.sleep(S3_CONSISTENCY_DELAY)

    log.debug("make_lambda_package: Done.")


def make_server_image(name, executables_path, instance):
    """Make an AMI that runs parameter servers.

    Args:
        name (str): The name to give the AMI.
        executables_path (str): An S3 path to a "directory" from which to get
            Cirrus' executables.
        instance (EC2Instance): The instance to use to set up the image. Should
            use an AMI produced by `make_build_image`.
    """
    assert executables_path.startswith("s3://")

    log = logging.getLogger("cirrus.automate.make_server_image")

    log.debug("make_server_image: Checking for already-existent images.")
    Instance.delete_images(name)

    log.debug("make_server_image: Putting parameter_server executable on instance.")
    instance.download_s3(executables_path + "/parameter_server", "~/parameter_server")

    log.debug("make_server_image: Setting permissions of executable.")
    instance.run_command("chmod +x ~/parameter_server")

    log.debug("make_server_image: Creating image from instance.")
    instance.save_image(name)

    log.debug("make_server_image: Done.")


def make_lambda(name, lambda_package_path, concurrency=-1):
    """Make a worker Lambda function.

    Replaces any existing Lambda function with the same name.

    Args:
        name (str): The name to give the Lambda.
        lambda_package_path (str): An S3 path to a Lambda ZIP package produced
            by `make_lambda_package`.
        concurrency (int): The number of reserved concurrent executions to
            allocate to the Lambda. If omitted or -1, the Lambda will use the
            account's unreserved concurrent executions in the region.
    """
    assert isinstance(concurrency, (int, long))
    assert concurrency >= -1

    # TODO: Make region an argument.
    log = logging.getLogger("cirrus.automate.make_lambda")

    log.debug("make_lambda: Initializing Lambda and IAM.")
    lamb = boto3.client("lambda", BUILD_INSTANCE["region"])
    iam = boto3.resource("iam", BUILD_INSTANCE["region"])

    log.debug("make_lambda: Deleting any existing Lambda.")
    try:
        lamb.delete_function(FunctionName=name)
    except Exception:
        # This is a hack. An error may be caused by something other than the
        #   Lambda not existing.
        pass

    log.debug("make_lambda: Deleting any existing IAM role.")
    try:
        role = iam.Role(name)
        for policy in role.attached_policies.all():
            role.detach_policy(PolicyArn=policy.arn)
        role.delete()
    except Exception:
        # This is a hack. An error may be caused by something other than the
        #   Lambda not existing.
        pass

    log.debug("make_lambda: Creating IAM role")
    role = iam.create_role(
        RoleName=name,
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
    role.attach_policy(PolicyArn=S3_READ_ONLY_ARN)
    role.attach_policy(PolicyArn=CLOUDWATCH_WRITE_ARN)

    log.debug("make_lambda: Waiting for changes to propogate.")
    # HACK: IAM is eventually consistent, so we sleep and hope that the role
    #   changes take effect in the meantime. But the delay distribtuion is
    #   heavy-tailed, so we actually need a retry mechanism.
    time.sleep(IAM_CONSISTENCY_DELAY)

    log.debug("make_lambda: Uploading ZIP package and creating Lambda.")
    bucket, key = _split_s3_url(lambda_package_path)
    lamb.create_function(
        FunctionName=name,
        Runtime=LAMBDA_RUNTIME,
        Role=role.arn,
        Handler=LAMBDA_HANDLER_FQID,
        Code={
            "S3Bucket": bucket,
            "S3Key": key
        },
        Timeout=LAMBDA_TIMEOUT,
        MemorySize=LAMBDA_SIZE
    )

    if concurrency != -1:
        log.debug("make_lambda: Allocating reserved concurrent executions to "
                  "the Lambda.")
        lamb.put_function_concurrency(
            FunctionName=name,
            ReservedConcurrentExecutions=concurrency
        )

    log.debug("make_lambda: Done.")


def concurrency_limit(lambda_name):
    """Get the concurrency limit of a Lambda.

    This is the number of reserved concurrent executions allocated to the
        Lambda, or the number of unreserved concurrent executions available in
        the region if none are allocated to it.

    Returns:
        int: The concurrency limit.
    """
    log = logging.getLogger("cirrus.automate.concurrency_limit")

    log.debug("concurrency_limit: Querying the Lambda's concurrency limit.")
    response = lamb.get_function(FunctionName=lambda_name)

    # TODO: This does not properly handle the case where there is no limit.
    return response["Concurrency"]["ReservedConcurrentExecutions"]


def launch_worker(lambda_name, config, num_workers, ps):
    """Launch a worker.

    Blocks until the worker terminates.

    Args:
        lambda_name (str): The name of a worker Lambda function.
        config (str): A configuration for the worker.
        num_workers (int): The total number of workers that are being launched.
        ps (ParameterServer): The parameter server that the worker should use.

    Raises:
        RuntimeError: If the invocation of the Lambda function fails.
    """
    log = logging.getLogger("cirrus.automate.launch_worker")

    log.debug("launch_worker: Invoking Lambda.")
    payload = {
        "config": config,
        "num_workers": num_workers,
        "ps_ip": ps.public_ip(),
        "ps_port": ps.ps_port()
    }
    response = lamb.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps(payload)
    )
    if response["StatusCode"] != 200:
        # TODO: We should probably do something with the body of the response,
        #   either print it or include it in the error.
        raise RuntimeError("The invocation failed!")

    log.debug("launch_worker: Done.")


def maintain_workers(n, lambda_name, config, ps, stop_event):
    """Maintain a fixed-size fleet of workers.

    Args:
        n (int): The number of workers.
        lambda_name (str): As for `launch_worker`.
        config (str): As for `launch_worker`.
        parameter_server (ParameterServer): As for `launch_worker`.
        stop_event (threading.Event): An event indicating that the worker fleet
            should no longer be refilled.
    """
    def maintain_one():
        while not stop_event.is_set():
            launch_worker(lambda_name, config, n, ps)


    def thread_name(i):
        return "maintain_workers.maintain_one " \
               "[ps_public_ip=%s, ps_port=%d, worker_index=%d]" \
               % (ps.public_ip(), ps.ps_port(), i)


    threads = [threading.Thread(target=maintain_one, name=thread_name(i))
               for i in range(n)]
    [thread.start() for thread in threads]


def launch_server(instance):
    """Launch a parameter server.

    Args:
        instance (EC2Instance): The instance on which to launch the server.
            Should use an AMI produced by `make_server_image`.
    """
    pass


def build():
    """Build Cirrus.

    Publishes Cirrus' executables, a worker Lambda ZIP package, and a parameter
        server AMI on S3.
    """
    make_build_image(BUILD_IMAGE_NAME)
    with Instance(**BUILD_INSTANCE) as instance:  # FIXME
        make_executables(EXECUTABLES_PATH, instance)
        make_lambda_package(LAMBDA_PACKAGE_PATH, EXECUTABLES_PATH)
        make_server_image(SERVER_IMAGE_NAME, EXECUTABLES_PATH, instance)


def deploy():
    """Deploy Cirrus.

    Creates a worker Lambda function.
    """
    make_lambda(LAMBDA_NAME, LAMBDA_PACKAGE_PATH)


def _split_s3_url(url):
    assert url.startswith("s3://")

    bucket = url[len("s3://"):].split("/")[0]
    key = url[len("s3://") + len(bucket) + 1:]
    return bucket, key


if __name__ == "__main__":
    log = logging.getLogger("cirrus")
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.StreamHandler(sys.stdout))

    #make_build_image("build_image", replace=True)
    #config = dict(BUILD_INSTANCE)
    #del config["ami_id"]
    #instance = Instance("test", ami_name="build_image", **config)
    #instance.start()
    #make_executables("s3://cirrus-public", instance)
    #make_lambda_package("s3://cirrus-public/lambda-package/v1", "s3://cirrus-public")
    #make_server_image("server_image", "s3://cirrus-public", instance)
    make_lambda("test", "s3://cirrus-public/lambda-package/v1")
