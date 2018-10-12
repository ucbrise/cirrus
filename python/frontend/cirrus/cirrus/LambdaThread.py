# Run a lambda function on AWS

import time
import json
import random
import boto3
from botocore.exceptions import ClientError
from threading import Thread
from utils import retry_loop

def handle_lambda_exception(e):
    # Handle the TooManyRequestsException in the retry loop
    if e.response.get("Error", {}).get("Code") == "TooManyRequestsException":
        # Stop if you get a TooManyRequestsException
        print("{0} did not launch".format(name))
        raise e

class LambdaThread(Thread):
    def run(self):
        l_client = boto3.client("lambda")
        # Prevent lambdas from launching multiple times
        self.lamdba_dict["dupe_nonce"] = (random.random() * 1000000) // 1.0
        # Wrap the lambda invocation in a retry loop
        lambda_invocation = lambda : l_client.invoke(FunctionName="neel_lambda", InvocationType="RequestResponse", LogType="Tail",
            Payload=json.dumps(self.lamdba_dict))
        retry_loop(lambda_invocation, ClientError, handle_lambda_exception, 
            name="Lambda for chunk {0}".format(self.lamdba_dict["s3_key"]))
