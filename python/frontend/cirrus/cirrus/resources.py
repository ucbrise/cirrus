"""Caches AWS resources.
"""
import threading
import logging

import boto3
import botocore

from . import configuration


class ResourceManager(object):
    """A manager of cached AWS resources.
    """

    def __init__(self, region):
        """Create a resource manager.

        Resources will be initialized asynchronously, in a separate thread.

        Args:
            region (str): The region that resources be bound to.
        """
        self._region = region

        self._lambda_client_ready = threading.Event()
        self._lambda_client_no_retries_ready = threading.Event()
        self._iam_resource_ready = threading.Event()
        self._ec2_resource_ready = threading.Event()
        self._cloudwatch_logs_client_ready = threading.Event()
        self._s3_resource_ready = threading.Event()
        self._sts_client_ready = threading.Event()

        self._log = logging.getLogger("cirrus.automate.ResourceManager")

        t = threading.Thread(target=self._initialize,
                             name="ResourceManager")
        t.start()


    @property
    def lambda_client(self):
        """Get a Lambda client.

        Returns:
            botocore.client.BaseClient: The client.
        """
        self._lambda_client_ready.wait()
        return self._lambda_client


    @property
    def lambda_client_no_retries(self):
        """Get a Lambda client that does not retry requests.

        Returns:
            botocore.client.BaseClient: The client.
        """
        from . import automate
        self._lambda_client_no_retries_ready.wait()
        return self._lambda_client_no_retries


    @property
    def iam_resource(self):
        """Get an IAM resource.

        Returns:
            boto3.resources.base.ServiceResource: The resource.
        """
        self._iam_resource_ready.wait()
        return self._iam_resource


    @property
    def iam_client(self):
        """Get an IAM client.

        Returns:
            botocore.client.BaseClient: The client.
        """
        return self.iam_resource.meta.client


    @property
    def ec2_resource(self):
        """Get an EC2 resource.

        Returns:
            boto3.resources.base.ServiceResource: The resource.
        """
        self._ec2_resource_ready.wait()
        return self._ec2_resource


    @property
    def ec2_client(self):
        """Get an EC2 client.

        Returns:
            botocore.client.BaseClient: The client.
        """
        return self.ec2_resource.meta.client


    @property
    def cloudwatch_logs_client(self):
        """Get a Cloudwatch Logs client.

        Returns:
            botocore.client.BaseClient: The client.
        """
        self._cloudwatch_logs_client_ready.wait()
        return self._cloudwatch_logs_client


    @property
    def s3_resource(self):
        """Get an S3 resource.

        Returns:
            boto3.resources.base.ServiceResource: The resource."""
        self._s3_resource_ready.wait()
        return self._s3_resource


    @property
    def s3_client(self):
        """Get an S3 client.

        Returns:
            botocore.client.BaseClient: The client.
        """
        return self.s3_resource.meta.client


    @property
    def sts_client(self):
        """Get an STS client.

        Returns:
            botocore.client.BaseClient: The client.
        """
        self._sts_client_ready.wait()
        return self._sts_client


    def _initialize(self):
        from . import automate

        # Lambda client
        self._log.debug("Initializing Lambda client.")
        self._lambda_client = boto3.client("lambda", self._region)
        self._lambda_client_ready.set()

        # Lambda client with no retries
        self._log.debug("Initializing no-retries Lambda client.")
        config = botocore.config.Config(
            read_timeout=automate.LAMBDA_READ_TIMEOUT,
            retries={"max_attempts": 0}
        )
        self._lambda_client_no_retries = boto3.client("lambda", self._region,
                                                      config=config)
        self._lambda_client_no_retries_ready.set()

        # IAM resource
        self._log.debug("Initializing IAM resource.")
        self._iam_resource = boto3.resource( "iam", self._region)
        self._iam_resource_ready.set()

        # EC2 resource
        self._log.debug("Initializing EC2 resource.")
        self._ec2_resource = boto3.resource("ec2", self._region)
        self._ec2_resource_ready.set()

        # CloudWatch Logs client
        self._log.debug("Initializing Cloudwatch Logs client.")
        self._cloudwatch_logs_client = boto3.client("logs", self._region)
        self._cloudwatch_logs_client_ready.set()

        # S3 resource
        self._log.debug("Initializing S3 resource.")
        self._s3_resource = boto3.resource("s3", self._region)
        self._s3_resource_ready.set()

        # STS client
        self._log.debug("Initializing STS client.")
        self._sts_client = boto3.client("sts", self._region)
        self._sts_client_ready.set()


# If a region is configured, create the resource manager. If not, the setup
#   script will create it after the region is configured.
try:
    region = configuration.config(False)["aws"]["region"]
    resources = ResourceManager(region)
except KeyError:
    resources = None
