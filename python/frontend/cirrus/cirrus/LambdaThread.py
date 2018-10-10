# Run a lambda function on AWS

import time
import json
import random
import boto3
from botocore.exceptions import ClientError
from threading import Thread


class LambdaThread(Thread):
    def run(self):
        l_client = boto3.client("lambda")
        failure = 1
        overall = time.time()
        # Prevent lambdas from launching multiple times
        self.lamdba_dict["dupe_nonce"] = (random.random() * 1000000) // 1.0
        while failure < 4:
            # Retry up to 4 times
            try:
                t0 = time.time()
                l_client.invoke(FunctionName="neel_lambda", InvocationType="RequestResponse", LogType="Tail",
                                Payload=json.dumps(self.lamdba_dict))
                print("Lambda for chunk {0} completed this attempt in {1}, all attempts in {2}".format(
                    self.lamdba_dict["s3_key"], time.time() - t0, time.time() - overall))
                break
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") == "TooManyRequestsException":
                    # Stop if you get a TooManyRequestsException
                    print("Too many requests, lambda for chunk {0} did not launch".format(
                        self.lamdba_dict["s3_key"]))
                    raise e
                failure += 1
            except Exception as e:
                failure += 1
                if failure == 4:
                    raise e
            print("Lambda failed for chunk {0}: Launching attempt #{1}".format(
                self.lamdba_dict["s3_key"], failure))
