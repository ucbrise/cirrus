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
import inspect
import os
import random

import paramiko
import boto3

from . import handler
from . import configuration

# A configuration to use for EC2 instances that will be used to build Cirrus.
BUILD_INSTANCE = {
    "disk_size": 32,  # GB
    "typ": "c5.4xlarge",
    "username": "ec2-user"
}

# The type of instance to use for compilation.
BUILD_INSTANCE_TYPE = "c5.4xlarge"

# The disk size, in GB, to use for compilation.
BUILD_INSTANCE_SIZE = 32

# The type of instance to use for parameter servers.
SERVER_INSTANCE_TYPE = "m5a.2xlarge"

# The disk size, in GB, to use for parameter servers.
SERVER_INSTANCE_SIZE = 32

# The base AMI to use for making the Amazon Linux build image. Gives the AMI ID
#   for each supported region. This is "amzn-ami-hvm-2017.03.1.20170812
#   -x86_64-gp2", which is recommended by AWS as of Sep 27, 2018 for compiling
#   executables for Lambda.
AMAZON_BASE_IMAGES = {
    "us-west-1": "ami-3a674d5a",
    "us-west-2": "ami-aa5ebdd2"
}

# The base AMI to use for making the Ubuntu build image. Gives the AMI ID for
#   each supported region. This is "Ubuntu Server 18.04 LTS (HVM), SSD Volume
#   Type", found in the AWS console.
UBUNTU_BASE_IMAGES = {
    "us-west-1": "ami-063aa838bd7631e0b",
    "us-west-2": "ami-0bbe6b35405ecebdb"
}

# The ARN of an IAM policy that allows full access to S3.
S3_FULL_ACCESS_ARN = "arn:aws:iam::aws:policy/AmazonS3FullAccess"

# The ARN of an IAM policy that allows read-only access to S3.
S3_READ_ONLY_ARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

# The ARN of an IAM policy that allows write access to Cloudwatch logs.
CLOUDWATCH_WRITE_ARN = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"

# The base name of the bucket created by Cirrus in users' AWS accounts.
BUCKET_BASE_NAME = "cirrus-bucket"

# The estimated delay of IAM's eventual consistency, in seconds.
IAM_CONSISTENCY_DELAY = 20

# The filenames of Cirrus' executables.
EXECUTABLES = ("parameter_server", "ps_test", "csv_to_libsvm")

# The level of compression to use when creating the Lambda ZIP package.
LAMBDA_COMPRESSION = zipfile.ZIP_DEFLATED

# The name to give to the file containing the Lambda's handler code.
LAMBDA_HANDLER_FILENAME = "handler.py"

# The estimated delay of S3's eventual consistency, in seconds.
S3_CONSISTENCY_DELAY = 20

# The runtime for the worker Lambda.
LAMBDA_RUNTIME = "python3.6"

# The fully-qualified identifier of the handler of the worker Lambda.
LAMBDA_HANDLER_FQID = "handler.run"

# The maximum execution time to give the worker Lambda, in seconds. Capped by AWS at 5 minutes.
LAMBDA_TIMEOUT = 5 * 60

# The amount of memory (and in proportion, CPU/network) to give to the worker Lambda, in megabytes.
LAMBDA_SIZE = 3008

# The level of logs that the worker Lambda should write to CloudWatch.
LAMBDA_LOG_LEVEL = "DEBUG"

# The maximum number of generations of Lambdas that will be invoked to serve as
#   as the worker with a given ID.
MAX_LAMBDA_GENERATIONS = 10000

# The maximum number of workers that may work on a given experiment.
MAX_WORKERS_PER_EXPERIMENT = 1000


class ClientManager(object):
    """A manager of cached AWS clients.
    """

    def __init__(self):
        """Create a client manager.

        Clients will not yet be initialized.
        """
        self.clear_cache()
        self._log = logging.getLogger("cirrus.automate.ClientManager")


    @property
    def lamb(self):
        """Get a Lambda client.

        Initializes one if none is cached.


        Returns:
            botocore.client.BaseClient: The client.
        """
        if self._lamb is None:
            self._log.debug("ClientManager: Initializing Lambda client.")
            self._lamb = boto3.client(
                "lambda",
                configuration.config()["aws"]["region"]
            )
        return self._lamb


    @property
    def iam(self):
        """Get an IAM resource.

        Initializes one if none is cached.

        Returns:
            boto3.resources.base.ServiceResource: The resource.
        """
        if self._iam is None:
            self._log.debug("ClientManager: Initializing IAM resource.")
            self._iam = boto3.resource(
                "iam",
                configuration.config()["aws"]["region"]
            )
        return self._iam


    @property
    def ec2(self):
        """Get an EC2 client.

        Initializes one if none is cached.

        Returns:
            botocore.client.BaseClient: The client.
        """
        if self._ec2 is None:
            self._log.debug("ClientManager: Initializing EC2 client.")
            self._ec2 = boto3.client(
                "ec2",
                configuration.config()["aws"]["region"]
            )
        return self._ec2


    @property
    def cloudwatch_logs(self):
        """Get a Cloudwatch Logs client.

        Initializes one if none is cached.

        Returns:
            botocore.client.BaseClient: The client.
        """
        if self._cloudwatch_logs is None:
            self._log.debug("ClientManager: Initializing Cloudwatch Logs "
                            "client.")
            self._cloudwatch_logs = boto3.client(
                "logs",
                configuration.config()["aws"]["region"]
            )
        return self._cloudwatch_logs


    @property
    def s3(self):
        """Get an S3 resource.

        Initializes one if none is cached.

        Returns:
            boto3.resources.base.ServiceResource: The resource."""
        if self._s3 is None:
            self._log.debug("ClientManager: Initializing S3 resource.")
            self._s3 = boto3.resource(
                "s3", configuration.config()["aws"]["region"])
        return self._s3


    def clear_cache(self):
        """Clear any cached clients.
        """
        self._lamb = None
        self._iam = None
        self._ec2 = None
        self._cloudwatch_logs = None
        self._s3 = None


# Cached AWS clients to be used throughout this module.
clients = ClientManager()


class Instance(object):
    """An EC2 instance."""

    # The interval at which to poll for an AMI becoming available, in seconds.
    IMAGE_POLL_INTERVAL = 10

    # The maximum number of times to poll for an AMI becoming available.
    IMAGE_POLL_MAX = (5 * 60) // IMAGE_POLL_INTERVAL

    # The name of the key pair used by Instances.
    KEY_PAIR_NAME = "cirrus_key_pair"

    # The path at which to save the private key to Instances. May begin with a
    #   tilde.
    PRIVATE_KEY_PATH = "~/.ssh/cirrus_key_pair.pem"

    # The name of the security group used by Instances.
    SECURITY_GROUP_NAME = "cirrus_security_group"

    # The name of the role used by Instances.
    ROLE_NAME = "cirrus_instance_role"


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

        ec2 = boto3.resource("ec2", configuration.config()["aws"]["region"])

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

        ec2 = boto3.resource("ec2", configuration.config()["aws"]["region"])

        log.debug("delete_images: Describing images.")
        response = ec2.meta.client.describe_images(
            Filters=[{"Name": "name", "Values": [name]}], Owners=["self"])

        for info in response["Images"]:
            image_id = info["ImageId"]
            log.debug("delete_images: Deleting image %s." % image_id)
            ec2.Image(info["ImageId"]).deregister()

        log.debug("delete_images: Done.")


    @classmethod
    def set_up_key_pair(cls):
        """Create a key pair for use by `Instance`s.

        Deletes any existing key pair with the same name. Saves the private key
            to `~/cirrus_key_pair.pem`.
        """
        log = logging.getLogger("automate.Instance.set_up_key_pair")

        log.debug("set_up_key_pair: Checking for an existing key pair.")
        filter = {"Name": "key-name", "Values": [cls.KEY_PAIR_NAME]}
        response = clients.ec2.describe_key_pairs(Filters=[filter])
        if len(response["KeyPairs"]) > 0:
            log.debug("set_up_key_pair: Deleting an existing key pair.")
            clients.ec2.delete_key_pair(KeyName=cls.KEY_PAIR_NAME)

        log.debug("set_up_key_pair: Creating key pair.")
        response = clients.ec2.create_key_pair(KeyName=cls.KEY_PAIR_NAME)

        log.debug("set_up_key_pair: Saving private key.")
        path = os.path.expanduser(cls.PRIVATE_KEY_PATH)
        if not os.path.exists(os.path.dirname(path)):
            os.path.makedirs(os.path.dirname(path))
        with open(path, "w") as f:
            f.write(response["KeyMaterial"])

        log.debug("set_up_key_pair: Done.")


    @classmethod
    def set_up_security_group(cls):
        """Create a security group for use by `Instance`s.

        Deletes any existing security groups with the same name.
        """
        log = logging.getLogger("automate.Instance.set_up_security_group")

        log.debug("set_up_security_group: Checking for existing security "
                  "groups.")
        filter = {"Name": "group-name", "Values": [cls.SECURITY_GROUP_NAME]}
        response = clients.ec2.describe_security_groups(Filters=[filter])
        for group_info in response["SecurityGroups"]:
            log.debug("set_up_security_group: Deleting an existing security "
                      "group.")
            clients.ec2.delete_security_group(GroupId=group_info["GroupId"])

        log.debug("set_up_security_group: Creating security group.")
        group = clients.ec2.create_security_group(
            GroupName=cls.SECURITY_GROUP_NAME,
            Description="Generated by the Cirrus setup script. Lets all "
                        "inbound and outbound traffic through."
        )

        # Allow all outbound traffic so that Instances will be able to fetch
        #   software and data. Allow all inbound traffic so that we will be able
        #   to send messages to programs on Instances.
        log.debug("set_up_security_group: Configuring security group.")
        # An IpProtocol of -1 means "all protocols" and additionally implies
        #   "all ports".
        clients.ec2.authorize_security_group_ingress(
            GroupName=cls.SECURITY_GROUP_NAME, IpProtocol="-1",
            CidrIp="0.0.0.0/0")

        log.debug("set_up_security_group: Done.")


    @classmethod
    def set_up_role(cls):
        """Create a role for use by `Instance`s.

        Deletes any existing role with the same name.
        """
        log = logging.getLogger("automate.instance.set_up_role")

        log.debug("set_up_role: Checking for an existing role.")
        iam_client = clients.iam.meta.client
        # TODO: Could cause a problem under rare circumstances. We are assuming
        #   that there are less than 1000 roles in the account.
        roles_response = iam_client.list_roles()
        exists = False
        for role_info in roles_response["Roles"]:
            if role_info["RoleName"] == cls.ROLE_NAME:
                exists = True
                break
        if exists:
            log.debug("set_up_role: Listing the policies of existing role.")
            role_policy_response = iam_client.list_attached_role_policies(
                RoleName=cls.ROLE_NAME)
            log.debug("set_up_role: Detaching policies from existing role.")
            for policy_info in role_policy_response["AttachedPolicies"]:
                iam_client.detach_role_policy(
                    RoleName=cls.ROLE_NAME,
                    PolicyArn=policy_info["PolicyArn"]
                )
            log.debug("set_up_role: Deleting an existing role.")
            iam_client.delete_role(RoleName=cls.ROLE_NAME)

        log.debug("set_up_role: Creating role.")
        role = iam_client.create_role(
            RoleName=cls.ROLE_NAME,
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

        log.debug("set_up_role: Attaching policies to role.")
        iam_client.attach_role_policy(RoleName=cls.ROLE_NAME,
                                      PolicyArn=S3_FULL_ACCESS_ARN)
        log.debug("set_up_role: Done.")


    def __init__(self, name, disk_size, typ, username, ami_id=None,
                 ami_name=None, ami_public=False, spot_bid=None):
        """Define an EC2 instance.

        Args:
            name (str): Name for the instance. The same name will be used for
                the key pair and security group that get created.
            disk_size (int): Disk space for the instance, in GB.
            typ (str): Type for the instance.
            username (str): SSH username for the AMI.
            ami_id (str): ID of the AMI for the instance. If omitted or None, `ami_name` must be provided.
            ami_name (str): Name of the AMI for the instance. Only used if `ami_id` is not provided. The first AMI with
                the name `ami_name` owned by the AWS account is used.
            spot_bid (str): The spot instance bid to make, as a dollar amount
+                per hour. If omitted or None, the instance will not be spot.
        """
        self._name = name
        self._disk_size = disk_size
        self._ami_id = ami_id
        self._type = typ
        self._username = username
        self._spot_bid = spot_bid
        self._log = logging.getLogger("cirrus.automate.Instance")

        self._log.debug("__init__: Initializing EC2.")
        self._ec2 = boto3.resource("ec2", configuration.config()["aws"]["region"])

        if self._ami_id is None:
            self._log.debug("__init__: Resolving AMI name to AMI ID.")
            response = self._ec2.meta.client.describe_images(
                Filters=[{"Name": "name", "Values": [ami_name]}], Owners=["self"])
            if len(response["Images"]) > 0:
                self._ami_id = response["Images"][0]["ImageId"]
            else:
                raise RuntimeError("No AMIs with the given name were found.")

        self._instance_profile = None
        self.instance = None
        self._ssh_client = None
        self._sftp_client = None

        self._buffering_commands = False
        self._buffered_commands = []

        self._log.debug("__init__: Done.")


    def start(self):
        """Start the instance.

        If an instance with the same name is already running, it will be reused
            and no new instance will be started.

        When finished, call `cleanup`. `cleanup` will also be registered as an
            `atexit` cleanup function so that it will still be called despite
            any errors.
        """
        atexit.register(self.cleanup)

        self._log.debug("start: Checking if an instance with the same name is "
                        + "already running.")
        if not self._exists():
            self._log.debug("start: Calling _make_instance_profile.")
            self._make_instance_profile()

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

        # exec_command is asynchronous. The following waits for completion.
        self._log.debug("run_command: Waiting for completion.")

        self._log.debug("run_command: Fetching stdout and stderr.")
        stdout_data, stderr_data = stdout.read(), stderr.read()
        self._log.debug("run_command: stdout had length %d." % len(stdout_data))
        self._log.debug("run_command: stderr had length %d." % len(stderr_data))

        status = stdout.channel.recv_exit_status()
        self._log.debug("run_command: Exit code was %d." % status)

        self._log.debug("run_command: Done.")
        return status, stdout_data, stderr_data


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


    def upload_s3(self, src, dest, public):
        """Upload a file from this instance to S3.

        Args:
            src (str): A path to a file on this instance. If relative, then
                relative to the home folder of this instance's SSH user.
            dest (str): A path on S3 to upload to.
            public (bool): Whether to give the resulting S3 object the
                "public-read" ACL.
        """
        assert not src.startswith("s3://")
        assert dest.startswith("s3://")

        command = ["aws", "s3", "cp", src, dest]
        if public:
            command.extend(("--acl", "public-read"))
        self.run_command(" ".join(command))


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
            if self._instance_profile is not None:
                self._log.debug("cleanup: Deleting instance profile.")
                self._instance_profile.remove_role(RoleName=self.ROLE_NAME)
                self._instance_profile.delete()
                self._instance_profile = None
            self._log.debug("cleanup: Done.")
        except:
            MESSAGE = "An error occured during cleanup. Some EC2 resources " \
                  "may remain. Delete them manually."
            print("=" * len(MESSAGE))
            print(MESSAGE)
            print("=" * len(MESSAGE))
            raise sys.exc_info()[1]


    def _exists(self):
        self._log.debug("_exists: Listing instances.")
        name_filter = {
            "Name": "tag:Name",
            "Values": [self._name]
        }
        state_filter = {
            "Name": "instance-state-name",
            "Values": ["running"]
        }
        filters = [name_filter, state_filter]
        instances = list(self._ec2.instances.filter(Filters=filters))

        if len(instances) > 0:
            self._log.info("_exists: An existing instance with the same name "
                           "was found.")
            self.instance = instances[0]
            name = self._name + "_instance_profile"
            self._instance_profile = clients.iam.InstanceProfile(name)
            return True

        self._log.info("_exists: No existing instance with the same name "
                       "was found.")
        return False


    def _make_instance_profile(self):
        self._log.debug("_make_instance_profile: Creating instance profile.")
        name = self._name + "_instance_profile"
        self._instance_profile = clients.iam.create_instance_profile(
            InstanceProfileName=name)

        self._log.debug("_make_instance_profile: Adding role to instance " \
                        "profile.")
        self._instance_profile.add_role(RoleName=self.ROLE_NAME)

        self._log.debug("_make_instance_profile: Waiting for changes to take " \
                        "effect.")
        # IAM is eventually consistent, so we need to wait for our changes to be
        #   reflected. The delay distribution is heavy-tailed, so this might
        #   still error, rarely. The right way is to retry at an interval.
        time.sleep(IAM_CONSISTENCY_DELAY)

        self._log.debug("_make_instance_profile: Done.")


    def _start_and_wait(self):
        self._log.debug("_start_and_wait: Starting a new instance.")
        tag = {
            "Key": "Name",
            "Value": self._name
        }
        tag_spec = {
            "ResourceType": "instance",
            "Tags": [tag]
        }
        block_dev = {
            "DeviceName": "/dev/xvda",
            "Ebs": {
                "DeleteOnTermination": True,
                "VolumeSize": self._disk_size,
            }
        }
        create_args = {
            "BlockDeviceMappings": [block_dev],
            "KeyName": self.KEY_PAIR_NAME,
            "ImageId": self._ami_id,
            "InstanceType": self._type,
            "MinCount": 1,
            "MaxCount": 1,
            "SecurityGroups": [self.SECURITY_GROUP_NAME],
            "IamInstanceProfile": {"Name": self._instance_profile.name},
            "TagSpecifications": [tag_spec]
        }
        if self._spot_bid is not None:
            create_args["InstanceMarketOptions"] = {
                "MarketType": "spot",
                "SpotOptions": {
                    "MaxPrice": self._spot_bid,
                    "SpotInstanceType": "one-time",
                    "InstanceInterruptionBehavior": "terminate"
                }
            }
        instances = self._ec2.create_instances(**create_args)
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

        with open(os.path.expanduser(self.PRIVATE_KEY_PATH), "r") as f:
            key = paramiko.RSAKey.from_private_key(f)
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
    # The maximum amount of time that a parameter server can take to start, in
    #   seconds.
    MAX_START_TIME = 60

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


    def wait_until_started(self):
        """Block until this parameter server has started.

        The parameter server is considered to have started once attempts to
            connect to it begin succeeding.

        Raises:
            RuntimeError: If this parameter server takes too long to start (or,
                presumably, crashes).
        """
        total_attempts = self.MAX_START_TIME // handler.PS_CONNECTION_TIMEOUT

        for attempt in range(1, total_attempts + 1):
            self._log.debug("start: Making connection attempt #%d to %s."
                            % (attempt, self))
            start = time.time()
            if self.reachable():
                self._log.debug("start: %s launched." % self)
                return
            elapsed = time.time() - start

            remaining = handler.PS_CONNECTION_TIMEOUT - elapsed
            if remaining > 0:
                time.sleep(remaining)

        raise RuntimeError("%s appears not to have started successfully."
                           % self)


    def stop(self):
        for task in ("error", "ps"):
            kill_command = "kill -n 9 $(cat %s_%d.pid)" % (task, self.ps_port())
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


    def reachable(self):
        """Return whether this parameter server is reachable.

        This parameter server is reachable if attempts to connect to it succeed.

        Returns:
            bool: Whether this parameter server is reachable.
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(handler.PS_CONNECTION_TIMEOUT)
            sock.connect((self.public_ip(), self.ps_port()))
            sock.close()
            return True
        except:
            return False


    def __str__(self):
        """Return a string representation of this parameter server.

        Returns:
            str: The string representation.
        """
        return "ParameterServer@%s:%d" % (self.public_ip(), self.ps_port())


def make_amazon_build_image(name):
    """Make an Amazon Linux AMI suitable for compiling Cirrus on.

    Deletes any existing AMI with the same name. The resulting AMI will be
        private.

    Args:
        name (str): The name to give the AMI.
    """
    log = logging.getLogger("cirrus.automate.make_amazon_build_image")

    log.debug("make_amazon_build_image: Deleting any existing images with the "
              "same name.")
    Instance.delete_images(name)

    log.debug("make_amazon_build_image: Launching an instance.")
    region = configuration.config()["aws"]["region"]
    instance = Instance("cirrus_make_amazon_build_image",
                        ami_id=AMAZON_BASE_IMAGES[region],
                        disk_size=BUILD_INSTANCE_SIZE,
                        typ=BUILD_INSTANCE_TYPE,
                        username="ec2-user")
    instance.start()

    log.debug("make_amazon_build_image: Setting up the environment.")

    # Install some necessary packages.
    instance.run_command("yes | sudo yum install git "
        "glibc-static openssl-static.x86_64 zlib-static.x86_64 libcurl-devel")
    instance.run_command("yes | sudo yum groupinstall \"Development Tools\"")

    # The above installed a recent version of gcc, but an old version of g++.
    #   Install a newer version of g++.
    instance.run_command("yes | sudo yum remove gcc48-c++")
    instance.run_command("yes | sudo yum install gcc72-c++")

    # The above pulled in an old version of cmake. Install a newer version of
    #   cmake by compiling from source.
    instance.run_command(
        "wget https://cmake.org/files/v3.10/cmake-3.10.0.tar.gz")
    instance.run_command("tar -xvzf cmake-3.10.0.tar.gz")
    instance.run_command("cd cmake-3.10.0; ./bootstrap")
    instance.run_command("cd cmake-3.10.0; make -j 16")
    instance.run_command("cd cmake-3.10.0; sudo make install")

    # Install newer versions of as and ld.
    instance.run_command("yes | sudo yum install binutils")

    # The above pulled in an old version of make. Install a newer version of
    #   make by compiling from source.
    instance.run_command("wget https://ftp.gnu.org/gnu/make/make-4.2.tar.gz")
    instance.run_command("tar -xf make-4.2.tar.gz")
    instance.run_command("cd make-4.2; ./configure")
    instance.run_command("cd make-4.2; make -j 16")
    instance.run_command("cd make-4.2; sudo make install")
    instance.run_command("sudo ln -sf /usr/local/bin/make /usr/bin/make")

    # Compile glibc from source with static NSS. Use the resulting libpthread.a
    #   instead of the default.
    instance.run_command("git clone git://sourceware.org/git/glibc.git")
    instance.run_command("cd glibc; git checkout release/2.28/master")
    instance.run_command("mkdir glibc/build")
    instance.run_command("cd glibc/build; ../configure --disable-sanity-checks "
                         "--enable-static-nss --prefix ~/glibc_build")
    instance.run_command("cd glibc/build; make -j 16")
    instance.run_command("cd glibc/build; make install")
    instance.run_command("sudo cp ~/glibc_build/lib/libpthread.a "
                         "/usr/lib64/libpthread.a")

    log.debug("make_amazon_build_image: Saving the image.")
    instance.save_image(name)

    log.debug("make_amazon_build_image: Terminating the instance.")
    instance.cleanup()


def make_ubuntu_build_image(name):
    """Make an Ubuntu AMI suitable for compiling Cirrus on.

    Deletes any existing AMI with the same name. The resulting AMI will be
        private.

    Args:
        name (str): The name to give the AMI.
    """
    log = logging.getLogger("cirrus.automate.make_ubuntu_build_image")

    log.debug("make_ubuntu_build_image: Deleting any existing images with the "
              "same name.")
    Instance.delete_images(name)

    log.debug("make_ubuntu_build_image: Launching an instance.")
    region = configuration.config()["aws"]["region"]
    instance = Instance("cirrus_make_ubuntu_build_image",
                        ami_id=UBUNTU_BASE_IMAGES[region],
                        disk_size=BUILD_INSTANCE_SIZE,
                        typ=BUILD_INSTANCE_TYPE,
                        username="ubuntu")
    instance.start()

    log.debug("make_ubuntu_build_image: Setting up the environment.")
    # Why twice? Sometimes it doesn't work the first time. It might also just be
    #   a timing thing.
    instance.run_command("sudo apt-get update")
    instance.run_command("sudo apt-get update")
    instance.run_command("yes | sudo apt-get install build-essential cmake \
                          automake zlib1g-dev libssl-dev libcurl4-nss-dev \
                          bison libldap2-dev libkrb5-dev")
    instance.run_command("yes | sudo apt-get install awscli")

    log.debug("make_ubuntu_build_image: Saving the image.")
    instance.save_image(name)

    log.debug("make_ubuntu_build_image: Terminating the instance.")
    instance.cleanup()


def make_executables(path, image_name, username):
    """Compile Cirrus and publish its executables.

    Overwrites any existing S3 objects with the same name. The resulting S3
        objects will be public.

    Args:
        path (str): A S3 path to a "directory" in which to publish the
            executables.
        image_name (str): The name of the AMI to compile on.
        username (str): The SSH username to use with the AMI.
    """
    log = logging.getLogger("cirrus.automate.make_executables")

    log.debug("make_executables: Launching an instance.")
    instance = Instance("cirrus_make_executables",
                        ami_name=image_name,
                        disk_size=BUILD_INSTANCE_SIZE,
                        typ=BUILD_INSTANCE_TYPE,
                        username=username)
    instance.start()

    log.debug("make_executables: Building Cirrus.")
    instance.run_command("git clone https://github.com/jcarreira/cirrus.git")
    instance.run_command("cd cirrus; ./bootstrap.sh")
    instance.run_command("cd cirrus; make -j 16")

    log.debug("make_executables: Publishing executables.")
    for executable in EXECUTABLES:
        instance.upload_s3("~/cirrus/src/%s" % executable,
                           path + "/" + executable, public=True)

    log.debug("make_executables: Terminating the instance.")
    instance.cleanup()

    log.debug("make_executables: Done.")


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
        handler_source = inspect.getsource(handler)
        zip.writestr(info, handler_source)

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
    s3_client.upload_fileobj(file, bucket, key,
        ExtraArgs={"ACL": "public-read"})

    log.debug("make_lambda_package: Waiting for changes to take effect.")
    # Waits for S3's eventual consistency to catch up. Ideally, something more sophisticated would be used since the
    #   delay distribution is heavy-tailed. But this should in most cases ensure the package is visible on S3 upon
    #   return.
    time.sleep(S3_CONSISTENCY_DELAY)

    log.debug("make_lambda_package: Done.")


def make_server_image(name, executables_path):
    """Make an AMI that runs parameter servers.

    Args:
        name (str): The name to give the AMI.
        executables_path (str): An S3 path to a "directory" from which to get
            Cirrus' executables.
    """
    assert executables_path.startswith("s3://")

    log = logging.getLogger("cirrus.automate.make_server_image")

    log.debug("make_server_image: Checking for already-existent images.")
    Instance.delete_images(name)

    log.debug("make_server_image: Launching an instance.")
    region = configuration.config()["aws"]["region"]
    instance = Instance("cirrus_make_server_image",
                        ami_id=UBUNTU_BASE_IMAGES[region],
                        disk_size=SERVER_INSTANCE_SIZE,
                        typ=SERVER_INSTANCE_TYPE,
                        username="ubuntu")
    instance.start()

    log.debug("make_server_image: Installing the AWS CLI.")
    # Why twice? Sometimes it didn't know about the awscli package unless I
    #   updated twice. It might just be due to timing.
    instance.run_command("sudo apt update")
    instance.run_command("sudo apt update")
    instance.run_command("yes | sudo apt install awscli")

    log.debug("make_server_image: Putting parameter_server executable on instance.")
    instance.download_s3(executables_path + "/parameter_server", "~/parameter_server")

    log.debug("make_server_image: Setting permissions of executable.")
    instance.run_command("chmod +x ~/parameter_server")

    log.debug("make_server_image: Creating image from instance.")
    instance.save_image(name)

    log.debug("make_server_image: Terminating the instance.")
    instance.cleanup()

    log.debug("make_server_image: Done.")


def get_bucket_name():
    """Get the name of Cirrus' S3 bucket in the user's AWS account.

    Returns:
        str: The name.
    """
    log = logging.getLogger("cirrus.automate.get_bucket_name")

    log.debug("get_bucket_name: Retreiving account ID.")
    account_id = boto3.client("sts").get_caller_identity().get("Account")

    return BUCKET_BASE_NAME + "-" + account_id


def set_up_bucket():
    """Set up Cirrus' S3 bucket in the user's AWS account.
    """
    log = logging.getLogger("cirrus.automate.set_up_bucket")

    log.debug("set_up_bucket: Checking for existing bucket.")
    response = clients.s3.meta.client.list_buckets()
    exists = False
    bucket_name = get_bucket_name()
    for bucket_info in response["Buckets"]:
        if bucket_info["Name"] == bucket_name:
            exists = True
            break

    if exists:
        log.debug("set_up_bucket: Deleting contents of existing bucket.")
        bucket = clients.s3.Bucket(bucket_name)
        for obj in bucket.objects.all():
            obj.delete()
        log.debug("set_up_bucket: Deleting existing bucket.")
        bucket.delete()

    log.debug("set_up_bucket: Creating bucket.")
    bucket_config = {
        "LocationConstraint": configuration.config()["aws"]["region"]
    }
    clients.s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration=bucket_config
    )


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

    log = logging.getLogger("cirrus.automate.make_lambda")

    log.debug("make_lambda: Initializing Lambda and IAM.")

    log.debug("make_lambda: Deleting any existing Lambda.")
    try:
        clients.lamb.delete_function(FunctionName=name)
    except Exception:
        # This is a hack. An error may be caused by something other than the
        #   Lambda not existing.
        pass

    log.debug("make_lambda: Deleting any existing IAM role.")
    try:
        role = clients.iam.Role(name)
        for policy in role.attached_policies.all():
            role.detach_policy(PolicyArn=policy.arn)
        role.delete()
    except Exception:
        # This is a hack. An error may be caused by something other than the
        #   Lambda not existing.
        pass

    log.debug("make_lambda: Creating IAM role")
    role = clients.iam.create_role(
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

    log.debug("make_lambda: Copying package to user's bucket.")
    bucket_name = get_bucket_name()
    bucket = clients.s3.Bucket(bucket_name)
    src_bucket, src_key = _split_s3_url(lambda_package_path)
    src = {"Bucket": src_bucket, "Key": src_key}
    bucket.copy(src, src_key)

    log.debug("make_lambda: Creating Lambda.")
    clients.lamb.create_function(
        FunctionName=name,
        Runtime=LAMBDA_RUNTIME,
        Role=role.arn,
        Handler=LAMBDA_HANDLER_FQID,
        Code={
            "S3Bucket": bucket_name,
            "S3Key": src_key
        },
        Timeout=LAMBDA_TIMEOUT,
        MemorySize=LAMBDA_SIZE
    )

    if concurrency != -1:
        log.debug("make_lambda: Allocating reserved concurrent executions to "
                  "the Lambda.")
        clients.lamb.put_function_concurrency(
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
    response = clients.lamb.get_function(FunctionName=lambda_name)

    # TODO: This does not properly handle the case where there is no limit.
    return response["Concurrency"]["ReservedConcurrentExecutions"]


def clear_lambda_logs(lambda_name):
    """Clear the Cloudwatch logs for a given Lambda function.

    Args:
        lambda_name (str): The name of the Lambda function.
    """
    log = logging.getLogger("cirrus.automate.clear_lambda_logs")

    log.debug("clear_lambda_logs: Listing log groups.")
    name = "/aws/lambda/%s" % lambda_name
    response = clients.cloudwatch_logs.describe_log_groups(
        logGroupNamePrefix=name)

    log.debug("clear_lambda_logs: Deleting matching log groups.")
    for group_info in response["logGroups"]:
        clients.cloudwatch_logs.delete_log_group(
            logGroupName=group_info["logGroupName"])


def launch_worker(lambda_name, task_id, config, num_workers, ps):
    """Launch a worker.

    Blocks until the worker terminates.

    Args:
        lambda_name (str): The name of a worker Lambda function.
        task_id (int): The ID number of the task, to be used by the worker to
            register with the parameter server.
        config (str): A configuration for the worker.
        num_workers (int): The total number of workers that are being launched.
        ps (ParameterServer): The parameter server that the worker should use.

    Raises:
        RuntimeError: If the invocation of the Lambda function fails.
    """
    log = logging.getLogger("cirrus.automate.launch_worker")

    log.debug("launch_worker: Launching Task %d." % task_id)
    payload = {
        "config": config,
        "num_workers": num_workers,
        "ps_ip": ps.public_ip(),
        "ps_port": ps.ps_port(),
        "task_id": task_id,
        "log_level": LAMBDA_LOG_LEVEL
    }
    response = clients.lamb.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps(payload)
    )

    status = response["StatusCode"]
    message = "launch_worker: Task %d completed with status code %d." \
              % (task_id, status)
    if status == 200:
        log.debug(message)
    else:
        raise RuntimeError(message)


def maintain_workers(n, lambda_name, config, ps, stop_event, experiment_id):
    """Maintain a fixed-size fleet of workers.

    Args:
        n (int): The number of workers.
        lambda_name (str): As for `launch_worker`.
        config (str): As for `launch_worker`.
        parameter_server (ParameterServer): As for `launch_worker`.
        stop_event (threading.Event): An event indicating that no new
            generations of the workers in the fleet should be launched.
        experiment_id (int): The ID number of the experiment that these workers
            will work on.
    """
    assert n <= MAX_WORKERS_PER_EXPERIMENT

    def maintain_one(worker_id):
        """Maintain a single worker.

        Launches generation after generation of Lambdas to serve as the
            `worker_id`-th worker.

        Args:
            worker_id (int): The ID of the worker, in `[0, n)`.
        """
        generation = 0

        elapsed_sec = 0
        while not stop_event.is_set():
            assert generation < MAX_LAMBDA_GENERATIONS

            # the 2nd time onwards we sleep until we complete 5mins
            if elapsed_sec > 0 and elapsed_sec < 1 * 60:
                time_to_wait = 2 * 60 - elapsed_sec
                print("Sleeping for {}".format(time_to_wait))
                time.sleep(time_to_wait)

            start = time.time()

            task_id = worker_id * MAX_LAMBDA_GENERATIONS + generation
            launch_worker(lambda_name, task_id, config, n, ps)

            generation += 1

            elapsed_sec = time.time() - start

    base_id = experiment_id * MAX_WORKERS_PER_EXPERIMENT
    threads = []
    for worker_id in range(base_id, base_id + n):
        thread = threading.Thread(
            target=maintain_one,
            name="Worker %d" % worker_id,
            args=(worker_id,)
        )
        threads.append(thread)
        thread.start()


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
