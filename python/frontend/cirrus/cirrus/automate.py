import logging
import time
import tempfile
import socket
import sys
import io
import atexit

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
S3_ACCESS_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonS3FullAccess"

# The estimated delay of IAM's eventual consistency, in seconds.
IAM_CONSISTENCY_DELAY = 20


class Instance(object):
    """An EC2 instance."""

    def __init__(self, name, region, disk_size, ami_id, typ, username):
        """Define an EC2 instance.

        Args:
            name (str): Name for the instance. The same name will be used for
                the key pair and security group that get created.
            region (str): Region for the instance.
            disk_size (int): Disk space for the instance, in GB.
            ami_id (str): ID of the AMI for the instance.
            typ (str): Type for the instance.
            username (str): SSH username for the AMI.
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

        self._role = None
        self._instance_profile = None
        self._key_pair = None
        self._private_key = None
        self._security_group = None
        self._instance = None
        self._ssh_client = None
        self._sftp_client = None

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


    def run_command(self, command):
        """Run a command on this instance.

        Args:
            command (str): The command to run.

        Returns:
            tuple[int, bytes, bytes]: The exit code, stdout, and stderr,
                respectively, of the process.
        """
        if self._ssh_client is None:
            self._log.debug("run_command: Calling _connect_ssh.")
            self._connect_ssh()

        self._log.debug(f"run_command: Running `{command}`.")
        _, stdout, stderr = self._ssh_client.exec_command(command)

        self._log.debug("run_command: Waiting for completion.")
        # exec_command is asynchronous. This waits for completion.
        status = stdout.channel.recv_exit_status()
        self._log.debug(f"run_command: Exit code was {status}.")

        self._log.debug("run_command: Fetching stdout and stderr.")
        stdout, stderr = stdout.read(), stderr.read()
        self._log.debug(f"run_command: stdout had length {len(stdout)}.")
        self._log.debug(f"run_command: stderr had length {len(stderr)}.")

        self._log.debug("run_command: Done.")
        return status, stdout, stderr


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

        instance.run_command(" ".join((
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

        instance.run_command(" ".join((
            "aws",
            "s3",
            "cp",
            src,
            dest
        )))


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
            if self._instance is not None:
                self._log.debug("cleanup: Terminating instance.")
                self._instance.terminate()
                self._log.debug("cleanup: Waiting for instance to terminate.")
                self._instance.wait_until_terminated()
                self._instance = None
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
                self._role.detach_policy(PolicyArn=S3_ACCESS_POLICY_ARN)
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
        self._role.attach_policy(PolicyArn=S3_ACCESS_POLICY_ARN)

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
        # Allow TCP port 22 access from anywhere so that we can control the
        #   instance.
        self._security_group.authorize_ingress(
            IpProtocol="tcp",
            FromPort=22,
            ToPort=22,
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
        self._instance = instances[0]

        self._log.debug("_start_and_wait: Waiting for instance to enter " \
                        "running state.")
        self._instance.wait_until_running()

        self._log.debug("_start_and_wait: Fetching instance metadata.")
        # Reloads metadata about the instance. In particular, retreives its
        #   public_ip_address.
        self._instance.load()

        self._log.debug("_start_and_wait: Done.")


    def _connect_ssh(self, timeout=10, attempts=10):
        self._log.debug("_connect_ssh: Configuring.")
        key = paramiko.RSAKey.from_private_key(io.StringIO(self._private_key))
        self._ssh_client = paramiko.SSHClient()
        self._ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        for i in range(attempts):
            try:
                self._log.debug(f"_connect_ssh: Making connection attempt " \
                                f"#{i+1} out of {attempts}.")
                self._ssh_client.connect(
                    hostname=self._instance.public_ip_address,
                    username=self._username,
                    pkey=key,
                    timeout=timeout
                )
                self._ssh_client.get_transport().window_size = 2147483647
            except socket.timeout:
                self._log.debug("_connect_ssh: Connection attempt timed out " \
                                f"after {timeout}s.")
                pass
            except paramiko.ssh_exception.NoValidConnectionsError:
                self._log.debug("_connect_ssh: Connection attempt failed. " \
                                f"Sleeping for {timeout}s.")
                time.sleep(timeout)
                pass
            else:
                break
        else:
            pass  # FIXME


def make_build_image(name, replace=False):
    """Make an AMI sutiable for compiling Cirrus on.

    Args:
        name (str): The name to give the AMI.
        replace (bool): Whether to replace any existing AMI with the same name.
            If False or omitted and an AMI with the same name exists, nothing
            will be done.
    """
    pass


def make_executables(path, instance):
    """Compile Cirrus and publish its executables.

    Args:
        path (str): A S3 path to a "directory" in which to publish the
            executables.
        instance (EC2Instance): The instance on which to compile. Should use an
            AMI produced by `make_build_image`.
    """
    pass


def make_lambda_package(path, executables_path):
    """Make and publish the ZIP package for Cirrus' Lambda function.

    Args:
        path (str): An S3 path at which to publish the package.
        executables_path (str): An S3 path to a "directory" from which to get
            Cirrus' executables.
    """
    pass


def make_server_image(name, executables_path, instance):
    """Make an AMI that runs parameter servers.

    Args:
        name (str): The name to give the AMI.
        executables_path (str): An S3 path to a "directory" from which to get
            Cirrus' executables.
        instance (EC2Instance): The instance to use to set up the image. Should
            use an AMI produced by `make_build_image`.
    """
    pass


def make_lambda(name, lambda_package_path):
    """Make a worker Lambda function.

    Args:
        name (str): The name to give the Lambda.
        lambda_package_path (str): An S3 path to a Lambda ZIP package produced
            by `make_lambda_package`.
    """
    pass


def launch_worker(lambda_name):
    """Launch a worker.

    Args:
        lambda_name (str): The name of a worker Lambda function.
    """
    pass


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


if __name__ == "__main__":
    log = logging.getLogger("cirrus")
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.StreamHandler(sys.stdout))

    instance = Instance(name="testabc", **BUILD_INSTANCE)
    instance.start()
    instance.run_command("ls")
    instance.run_command("pwd")
