""" Run a lambda function on AWS """

import json
import random
from threading import Thread

import boto3
from botocore.exceptions import ClientError
from cirrus.utils import retry_loop


def handle_lambda_exception(exception):
    """ Handle the TooManyRequestsException in the retry loop """
    if exception.response.get("Error", {}).get("Code") \
        == "TooManyRequestsException":
        # Stop if you get a TooManyRequestsException
        print("Lambda did not launch, TooManyRequestsException")
        raise exception


class LambdaThread(Thread):
    """ Run a lambda function on AWS """

    def __init__(self):
        Thread.__init__(self)

    def run(self):
        l_client = boto3.client("lambda")
        # Prevent lambdas from launching multiple times
        self.lamdba_dict["dupe_nonce"] = (random.random() * 1000000) // 1.0
        # Call the lambda invocation in a retry loop

        def lambda_invocation():
            """ Wrap the lambda invocation in a closure """
            return l_client.invoke(
                FunctionName="neel_lambda",
                InvocationType="RequestResponse",
                LogType="Tail",
                Payload=json.dumps(self.lamdba_dict)
            )

        retry_loop(lambda_invocation, ClientError, handle_lambda_exception,
                   name="CHUNK_{0}_LAMBDA"
                   .format(self.lamdba_dict["s3_key"]))
