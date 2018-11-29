"""Caches AWS clients.
"""
import threading
import logging

import boto3
import botocore

from . import configuration


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
        self._lamb_no_retries_mutex = threading.Lock()
        self._iam_mutex = threading.Lock()
        self._ec2_mutex = threading.Lock()
        self._cloudwatch_logs_mutex = threading.Lock()
        self._s3_mutex = threading.Lock()
        self._sts_mutex = threading.Lock()

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
    def lamb_no_retries(self):
        """Get a Lambda client that does not retry requests.

        Initializes one if none is cached.

        Returns:
            botocore.client.BaseClient: The client.
        """
        from . import automate
        with self._lamb_no_retries_mutex:
            if self._lamb_no_retries is None:
                self._log.debug("ClientManager: Initializing no-retries Lambda "
                                "client.")
                region = configuration.config()["aws"]["region"]
                config = botocore.config.Config(
                    read_timeout=automate.LAMBDA_READ_TIMEOUT,
                    retries={"max_attempts": 0}
                )
                self._lamb_no_retries = boto3.client("lambda", region,
                                                      config=config)
        return self._lamb_no_retries


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
        return self.ec2_resource.meta.client


    @property
    def ec2_resource(self):
        """Get an EC2 resource.

        Initializes one if none is cached.

        Returns:
            boto3.resources.base.ServiceResource: The resource.
        """
        with self._ec2_mutex:
            if self._ec2 is None:
                self._log.debug("ClientManager: Initializing EC2 resource.")
                self._ec2 = boto3.resource(
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


    @property
    def sts(self):
        """Get an STS client.

        Initializes one if none is cached.

        Returns:
            botocore.client.BaseClient: The client.
        """
        with self._sts_mutex:
            if self._sts is None:
                self._log.debug("ClientManager: Initializing STS client.")
                self._sts = boto3.client("sts")
        return self._sts


    def clear_cache(self):
        """Clear any cached clients.
        """
        with self._lamb_mutex:
            self._lamb = None

        with self._lamb_no_retries_mutex:
            self._lamb_no_retries = None

        with self._iam_mutex:
            self._iam = None

        with self._ec2_mutex:
            self._ec2 = None

        with self._cloudwatch_logs_mutex:
            self._cloudwatch_logs = None

        with self._s3_mutex:
            self._s3 = None

        with self._sts_mutex:
            self._sts = None


clients = ClientManager()
