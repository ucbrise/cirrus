import logging
import time
import socket
import sys
import io
import zipfile
import pipes
import json
import threading
import inspect

import boto3

from . import handler
from . import configuration
from .instance import Instance

# A configuration to use for EC2 instances that will be used to build Cirrus.
BUILD_INSTANCE = {
    "disk_size": 32,  # GB
    "typ": "c5.4xlarge",
    "username": "ec2-user"
}

# The type of instance to use for compilation.
BUILD_INSTANCE_TYPE = "c4.4xlarge"

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
        # These mutexes synchronize reading/writing of the respective client
        #   attributes.
        self._lamb_mutex = threading.Lock()
        self._iam_mutex = threading.Lock()
        self._ec2_mutex = threading.Lock()
        self._cloudwatch_logs_mutex = threading.Lock()
        self._s3_mutex = threading.Lock()

        self.clear_cache()
        self._log = logging.getLogger("cirrus.automate.ClientManager")


    @property
    def lamb(self):
        """Get a Lambda client.

        Initializes one if none is cached.


        Returns:
            botocore.client.BaseClient: The client.
        """
        with self._lamb_mutex:
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
        with self._iam_mutex:
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
        with self._ec2_mutex:
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
        with self._cloudwatch_logs_mutex:
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
        with self._s3_mutex:
            if self._s3 is None:
                self._log.debug("ClientManager: Initializing S3 resource.")
                self._s3 = boto3.resource(
                    "s3", configuration.config()["aws"]["region"])
        return self._s3


    def clear_cache(self):
        """Clear any cached clients.
        """
        with self._lamb_mutex:
            self._lamb = None

        with self._iam_mutex:
            self._iam = None

        with self._ec2_mutex:
            self._ec2 = None

        with self._cloudwatch_logs_mutex:
            self._cloudwatch_logs = None

        with self._s3_mutex:
            self._s3 = None


# Cached AWS clients to be used throughout this module.
clients = ClientManager()


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
    instance.run_command("sudo cp ~/glibc_build/lib/libc.a /usr/lib64/libc.a")

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


def make_executables(path, image_owner_name, username):
    """Compile Cirrus and publish its executables.

    Overwrites any existing S3 objects with the same name. The resulting S3
        objects will be public.

    Args:
        path (str): A S3 path to a "directory" in which to publish the
            executables.
        image_owner_name (tuple[str, str]): The owner and name of the AMI to
            compile on. As for `Instance.__init__`.
        username (str): The SSH username to use with the AMI.
    """
    log = logging.getLogger("cirrus.automate.make_executables")

    log.debug("make_executables: Launching an instance.")
    instance = Instance("cirrus_make_executables",
                        ami_owner_name=image_owner_name,
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
        executables_path += "/amazon/parameter_server"
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
    instance.download_s3(executables_path + "/ubuntu/parameter_server",
                         "~/parameter_server")

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
