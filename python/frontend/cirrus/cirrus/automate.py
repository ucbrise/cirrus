import logging
import time
import io
import zipfile
import json
import threading
import inspect
import datetime

from . import handler
from . import configuration
from .instance import Instance
from . import utilities
from .resources import resources

# The type of instance to use for compilation.
BUILD_INSTANCE_TYPE = "c4.4xlarge"

# The disk size, in GB, to use for compilation.
BUILD_INSTANCE_SIZE = 32

# The type of instance to use for creating server images.
SERVER_INSTANCE_TYPE = "m4.2xlarge"

# The disk size, in GB, to use for parameter servers.
SERVER_INSTANCE_SIZE = 1

# The base AMI to use for making the Amazon Linux build image. Gives the AMI ID
#   for each supported region. This is "amzn-ami-hvm-2017.03.1.20170812
#   -x86_64-gp2", which is recommended by AWS as of Sep 27, 2018 for compiling
#   executables for Lambda.
AMAZON_BASE_IMAGES = {
    "us-east-1": "ami-4fffc834",
    "us-east-2": "ami-ea87a78f",
    "us-west-1": "ami-3a674d5a",
    "us-west-2": "ami-aa5ebdd2"
}

# The base AMI to use for making the Ubuntu build image. Gives the AMI ID for
#   each supported region. This is "Ubuntu Server 18.04 LTS (HVM), SSD Volume
#   Type", found in the AWS console.
UBUNTU_BASE_IMAGES = {
    "us-east-1": "ami-0ac019f4fcb7cb7e6",
    "us-east-2": "ami-0f65671a86f061fcd",
    "us-west-1": "ami-063aa838bd7631e0b",
    "us-west-2": "ami-0bbe6b35405ecebdb"
}

# The ARN of an IAM policy that allows read-only access to S3.
S3_READ_ONLY_ARN = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"

# The ARN of an IAM policy that allows write access to Cloudwatch logs.
CLOUDWATCH_WRITE_ARN = "arn:aws:iam::aws:policy/service-role/" \
                       "AWSLambdaBasicExecutionRole"

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

# The maximum execution time to give the worker Lambda, in seconds.
LAMBDA_TIMEOUT = 5 * 60

# The maximum amount of time that we will wait after invoking a Lambda in order
#   to read its output, in seconds.
LAMBDA_READ_TIMEOUT = LAMBDA_TIMEOUT + 30

# The level of logs that the worker Lambda should write to CloudWatch.
LAMBDA_LOG_LEVEL = "DEBUG"

# The maximum number of generations of Lambdas that will be invoked to serve as
#   as the worker with a given ID.
MAX_LAMBDA_GENERATIONS = 10000

# The maximum number of workers that may work on a given experiment. This is
#   1000 because the worker ID is given 3 digits in the task ID that workers use
#   to register.
MAX_WORKERS_PER_EXPERIMENT = 1000

# The minimum number of seconds that must pass between two Lambda invocations
#   for a worker.
MIN_GENERATION_TIME = 10

# The minimum number of concurrent executions that AWS requires an account to
#   keep unreserved. Current as of 11/21/18.
_MINIMUM_UNRESERVED_CONCURRENCY = 100


def make_amazon_build_image(name):
    """Make an Amazon Linux AMI suitable for compiling Cirrus on.

    Deletes any existing AMI with the same name. The resulting AMI will be
        private.

    Args:
        name (str): The name to give the AMI.
    """
    log = logging.getLogger("cirrus.automate.make_amazon_build_image")

    log.debug("Deleting any existing images with the same name.")
    Instance.delete_images(name)

    log.debug("Launching an instance.")
    region = configuration.config()["aws"]["region"]
    instance = Instance("cirrus_make_amazon_build_image",
                        ami_id=AMAZON_BASE_IMAGES[region],
                        disk_size=BUILD_INSTANCE_SIZE,
                        typ=BUILD_INSTANCE_TYPE,
                        username="ec2-user")
    instance.start()

    log.debug("Setting up the environment.")

    # Install some necessary packages.
    instance.run_command("yes | sudo yum install git "
        "glibc-static openssl-static.x86_64 zlib-static.x86_64 libcurl-devel")
    instance.run_command("yes | sudo yum groupinstall \"Development Tools\"")

    # Install some useful tools.
    instance.run_command("yes | sudo yum install gdb")
    instance.run_command("yes | sudo yum install htop")
    instance.run_command("yes | sudo yum install mosh")


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

    log.debug("Saving the image.")
    instance.save_image(name, False)

    log.debug("Terminating the instance.")
    instance.cleanup()


def make_ubuntu_build_image(name):
    """Make an Ubuntu AMI suitable for compiling Cirrus on.

    Deletes any existing AMI with the same name. The resulting AMI will be
        private.

    Args:
        name (str): The name to give the AMI.
    """
    log = logging.getLogger("cirrus.automate.make_ubuntu_build_image")

    log.debug("Deleting any existing images with the same name.")
    Instance.delete_images(name)

    log.debug("Launching an instance.")
    region = configuration.config()["aws"]["region"]
    instance = Instance("cirrus_make_ubuntu_build_image",
                        ami_id=UBUNTU_BASE_IMAGES[region],
                        disk_size=BUILD_INSTANCE_SIZE,
                        typ=BUILD_INSTANCE_TYPE,
                        username="ubuntu")
    instance.start()

    log.debug("Setting up the environment.")

    # Sometimes `apt-get update` doesn't work, returning exit code 100.
    while True:
        status, _, _ = instance.run_command("sudo apt-get update", False)
        if status == 0:
            break

    instance.run_command("yes | sudo apt-get install build-essential cmake \
                          automake zlib1g-dev libssl-dev libcurl4-nss-dev \
                          bison libldap2-dev libkrb5-dev")
    instance.run_command("yes | sudo apt-get install awscli")

    # Install some useful tools.
    instance.run_command("yes | sudo apt-get install gdb")
    instance.run_command("yes | sudo apt-get install htop")
    instance.run_command("yes | sudo apt-get install mosh")

    log.debug("Saving the image.")
    instance.save_image(name, False)

    log.debug("Terminating the instance.")
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

    log.debug("Launching an instance.")
    instance = Instance("cirrus_make_executables",
                        ami_owner_name=image_owner_name,
                        disk_size=BUILD_INSTANCE_SIZE,
                        typ=BUILD_INSTANCE_TYPE,
                        username=username)
    instance.start()

    log.debug("Building Cirrus.")
    instance.run_command("git clone https://github.com/jcarreira/cirrus.git")
    instance.run_command("cd cirrus; ./bootstrap.sh")
    instance.run_command("cd cirrus; make -j 16")

    log.debug("Publishing executables.")
    for executable in EXECUTABLES:
        instance.upload_s3("~/cirrus/src/%s" % executable,
                           path + "/" + executable, public=True)

    log.debug("Terminating the instance.")
    instance.cleanup()

    log.debug("Done.")


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

    log.debug("Initializing ZIP file.")
    file = io.BytesIO()
    with zipfile.ZipFile(file, "w", LAMBDA_COMPRESSION) as zip:
        log.debug("Writing handler.")
        info = zipfile.ZipInfo(LAMBDA_HANDLER_FILENAME)
        info.external_attr = 0o777 << 16  # Gives execute permission.
        handler_source = inspect.getsource(handler)
        zip.writestr(info, handler_source)

        log.debug("Initializing S3.")
        executable = io.BytesIO()

        log.debug("Downloading executable.")
        executables_path += "/amazon/parameter_server"
        bucket, key = _split_s3_url(executables_path)
        resources.s3_client.download_fileobj(bucket, key, executable)

        log.debug("Writing executable.")
        info = zipfile.ZipInfo("parameter_server")
        info.external_attr = 0o777 << 16  # Gives execute permission.
        executable.seek(0)
        zip.writestr(info, executable.read())

    log.debug("Uploading package.")
    file.seek(0)
    bucket, key = _split_s3_url(path)
    resources.s3_client.upload_fileobj(file, bucket, key,
                                            ExtraArgs={"ACL": "public-read"})

    log.debug("Waiting for changes to take effect.")
    # Waits for S3's eventual consistency to catch up. Ideally, something more
    #   sophisticated would be used since the delay distribution is
    #   heavy-tailed. But this should in most cases ensure the package is
    #   visible on S3 upon return.
    time.sleep(S3_CONSISTENCY_DELAY)

    log.debug("Done.")


def make_server_image(name, executables_path):
    """Make an AMI that runs parameter servers.

    Args:
        name (str): The name to give the AMI.
        executables_path (str): An S3 path to a "directory" from which to get
            Cirrus' executables.
    """
    assert executables_path.startswith("s3://")

    log = logging.getLogger("cirrus.automate.make_server_image")

    log.debug("Checking for already-existent images.")
    Instance.delete_images(name)

    log.debug("Launching an instance.")
    region = configuration.config()["aws"]["region"]
    instance = Instance("cirrus_make_server_image",
                        ami_id=UBUNTU_BASE_IMAGES[region],
                        disk_size=SERVER_INSTANCE_SIZE,
                        typ=SERVER_INSTANCE_TYPE,
                        username="ubuntu")
    instance.start()

    log.debug("Putting parameter_server executable on instance.")
    instance.download_s3(executables_path + "/ubuntu/parameter_server",
                         "~/parameter_server")

    log.debug("Setting permissions of executable.")
    instance.run_command("chmod +x ~/parameter_server")

    log.debug("Creating image from instance.")
    instance.save_image(name, False)

    log.debug("Terminating the instance.")
    instance.cleanup()

    log.debug("Done.")


def get_bucket_name():
    """Get the name of Cirrus' S3 bucket in the user's AWS account.

    Returns:
        str: The name.
    """
    log = logging.getLogger("cirrus.automate.get_bucket_name")

    log.debug("Retreiving account ID.")
    account_id = resources.sts_client.get_caller_identity().get("Account")

    return BUCKET_BASE_NAME + "-" + account_id


def set_up_bucket():
    """Set up Cirrus' S3 bucket in the user's AWS account.
    """
    log = logging.getLogger("cirrus.automate.set_up_bucket")

    log.debug("Checking for existing bucket.")
    response = resources.s3_client.list_buckets()
    exists = False
    bucket_name = get_bucket_name()
    for bucket_info in response["Buckets"]:
        if bucket_info["Name"] == bucket_name:
            exists = True
            break

    if exists:
        log.debug("Deleting contents of existing bucket.")
        bucket = resources.s3_resource.Bucket(bucket_name)
        for obj in bucket.objects.all():
            obj.delete()
        log.debug("Deleting existing bucket.")
        bucket.delete()

    log.debug("Creating bucket.")
    constraint = configuration.config()["aws"]["region"]
    bucket_config = {
        "LocationConstraint": constraint
    }
    # Per https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region, no
    #   constraint should be provided when referring to the us-east-1 region.
    if constraint == "us-east-1":
        resources.s3_resource.create_bucket(
            Bucket=bucket_name
        )
    else:
        resources.s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration=bucket_config
        )


def get_available_concurrency():
    """Get the number of unreserved concurrent executions available in the
        current AWS account.

    Returns:
        int: The number of executions.
    """
    log = logging.getLogger("cirrus.automate.get_available_concurrency")

    log.debug("Getting account settings.")
    response = resources.lambda_client.get_account_settings()
    unreserved = response["AccountLimit"]["UnreservedConcurrentExecutions"]
    available = unreserved - _MINIMUM_UNRESERVED_CONCURRENCY

    log.debug("Done.")
    return available


def set_up_lambda_role(name):
    """Set up the IAM role for the worker Lambda function.

    Deletes any existing role with the same name. This role gives read access to
        S3 and full access to Cloudwatch Logs.

    Args:
        name (str): The name to give the role.
    """
    log = logging.getLogger("cirrus.automate.set_up_lambda_role")

    log.debug("Checking for an already-existing role.")
    try:
        role = resources.iam_resource.Role(name)
        for policy in role.attached_policies.all():
            role.detach_policy(PolicyArn=policy.arn)
        role.delete()
        log.info("There was an already-existing role.")
    except Exception:
        # FIXME: This is a hack. An error may be caused by something other than
        #   the role not existing. We should catch only that specific error.
        log.info("There was not an already-existing role.")

    log.debug("Creating role.")
    role = resources.iam_resource.create_role(
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

    log.debug("Done.")


def make_lambda(name, lambda_package_path, lambda_size, concurrency=-1):
    """Make a worker Lambda function.

    Replaces any existing Lambda function with the same name.

    Args:
        name (str): The name to give the Lambda.
        lambda_package_path (str): An S3 path to a Lambda ZIP package produced
            by `make_lambda_package`.
        lambda_size (int): The amount of memory (in MB) to give to the Lambda
            function. Must be a size supported by AWS. As of 11/24/18, the
            supported sizes are multiples of 64 in [128, 3008].
        concurrency (int): The number of reserved concurrent executions to
            allocate to the Lambda. If omitted or -1, the Lambda will use the
            account's unreserved concurrent executions in the region.
    """
    assert 128 <= lambda_size <= 3008, \
        "lambda_size %d is not in [128, 3008]." % lambda_size
    assert lambda_size % 64 == 0, \
        "lambda_size %d is not divisible by 64." % lambda_size

    from . import setup

    assert isinstance(concurrency, (int, long))
    assert concurrency >= -1

    log = logging.getLogger("cirrus.automate.make_lambda")

    log.debug("Deleting any existing Lambda.")
    try:
        resources.lambda_client.delete_function(FunctionName=name)
    except Exception:
        # This is a hack. An error may be caused by something other than the
        #   Lambda not existing.
        pass

    log.debug("Copying package to user's bucket.")
    bucket_name = get_bucket_name()
    bucket = resources.s3_resource.Bucket(bucket_name)
    src_bucket, src_key = _split_s3_url(lambda_package_path)
    src = {"Bucket": src_bucket, "Key": src_key}
    bucket.copy(src, src_key)

    log.debug("Creating Lambda.")
    role_arn = resources.iam_resource.Role(setup.LAMBDA_ROLE_NAME).arn
    resources.lambda_client.create_function(
        FunctionName=name,
        Runtime=LAMBDA_RUNTIME,
        Role=role_arn,
        Handler=LAMBDA_HANDLER_FQID,
        Code={
            "S3Bucket": bucket_name,
            "S3Key": src_key
        },
        Timeout=LAMBDA_TIMEOUT,
        MemorySize=lambda_size
    )

    if concurrency != -1:
        log.debug("Allocating reserved concurrent executions to the Lambda.")
        resources.lambda_client.put_function_concurrency(
            FunctionName=name,
            ReservedConcurrentExecutions=concurrency
        )

    log.debug("Done.")


def delete_lambda(name):
    """Delete a Lambda function.

    Args:
        name (str): The name of the Lambda function.
    """
    log = logging.getLogger("cirrus.automate.delete_lambda")

    log.debug("Deleting Lambda function %s." % name)
    resources.lambda_client.delete_function(FunctionName=name)


@utilities.jittery_exponential_backoff(("TooManyRequestsException",), 2, 4, 3)
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

    log.debug("Launching Task %d." % task_id)
    payload = {
        "config": config,
        "num_workers": num_workers,
        "ps_ip": ps.public_ip(),
        "ps_port": ps.ps_port(),
        "task_id": task_id,
        "log_level": LAMBDA_LOG_LEVEL
    }
    response = resources.lambda_client_no_retries.invoke(
        FunctionName=lambda_name,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps(payload)
    )

    status = response["StatusCode"]
    message = "Task %d completed with status code %d." \
              % (task_id, status)
    if status == 200:
        log.debug(message)
    else:
        raise RuntimeError(message)


def maintain_workers(n, config, ps, stop_event, experiment_id, lambda_size):
    """Maintain a fixed-size fleet of workers.

    Creates a worker Lambda function to invoke.

    Args:
        n (int): The number of workers.
        config (str): As for `launch_worker`.
        ps (ParameterServer): As for `launch_worker`.
        stop_event (threading.Event): An event indicating that no new
            generations of the workers in the fleet should be launched.
        experiment_id (int): The ID number of the experiment that these workers
            will work on.
        lambda_size (int): As for `make_lambda`.
    """
    # Imported here to prevent a circular dependency issue.
    from . import setup

    # See the documentation for the constant.
    assert n <= MAX_WORKERS_PER_EXPERIMENT

    # Creates a Lambda function to invoke. Names it uniquely with the
    #   `experiment_id`, current date, and current time.
    now = datetime.datetime.now()
    lambda_id = "%d_%d-%d-%d_%d-%d-%d-%d" % (experiment_id, now.year, now.month,
        now.day, now.hour, now.minute, now.second, now.microsecond)
    lambda_name = setup.LAMBDA_NAME_PREFIX + "_" + lambda_id
    lambda_package_path = setup.PUBLISHED_BUILD + "/lambda_package"
    concurrency = int(configuration.config()["aws"]["lambda_concurrency_limit"])
    make_lambda(lambda_name, lambda_package_path, lambda_size, concurrency)


    def clean_up():
        """Clean up after the run.

        Deletes the Lambda that was created.
        """
        stop_event.wait()
        delete_lambda(lambda_name)


    def maintain_one(worker_id):
        """Maintain a single worker.

        Launches generation after generation of Lambdas to serve as the
            `worker_id`-th worker.

        Args:
            worker_id (int): The ID of the worker, in `[0, n)`.
        """
        generation = 0

        while not stop_event.is_set():
            assert generation < MAX_LAMBDA_GENERATIONS

            task_id = worker_id * MAX_LAMBDA_GENERATIONS + generation
            start = time.time()
            launch_worker(lambda_name, task_id, config, n, ps)

            duration = time.time() - start
            if duration < MIN_GENERATION_TIME:
                time.sleep(MIN_GENERATION_TIME - duration)

            generation += 1


    # Start the `clean_up` thread. Return immediately.
    thread_name = "Exp #%02d Cleanup" % experiment_id
    thread = threading.Thread(target=clean_up, name=thread_name)
    thread.start()

    # Start one `maintain_one` thread per worker desired. Return immediately.
    base_id = experiment_id * MAX_WORKERS_PER_EXPERIMENT
    for worker_id in range(base_id, base_id + n):
        thread_name = "Exp #%02d Wkr #%02d" % (experiment_id, worker_id)
        thread = threading.Thread(target=maintain_one, name=thread_name,
                                  args=(worker_id,))
        thread.start()


def _split_s3_url(url):
    assert url.startswith("s3://")

    bucket = url[len("s3://"):].split("/")[0]
    key = url[len("s3://") + len(bucket) + 1:]
    return bucket, key
