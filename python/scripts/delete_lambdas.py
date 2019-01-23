"""Delete all Lambda functions in a region.
"""
import boto3

REGION = "us-west-2"

client = boto3.client("lambda", REGION)
response = client.list_functions()
for function_info in response["Functions"]:
    resp = raw_input("Delete %s?\ny/n: " % function_info["FunctionName"])
    if resp == "y":
        client.delete_function(FunctionName=function_info["FunctionName"])
