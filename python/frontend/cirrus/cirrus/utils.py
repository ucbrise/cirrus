

import random
import boto3


lc = boto3.client('lambda')
iam_client = boto3.client('iam')

def get_random_color():
    r = lambda: random.randint(0,255)
    return 'rgb(%d, %d, %d)' % (r(),r(),r())


def create_lambda(size=128):
    with open('bundle.zip', 'rb') as f:
        zipped_code = f.read()
    
    role = iam_client.get_role(Rolename="fix_lambda_role")


    fn = "testfunct1_%d" % size

    lc.create_function(
            FunctionName=fn,
            Runtime="Python2.7"
            Handler='handler.handler'
            Code=dict(Zipfile=zipped_code)
            Timeout=300,
            Role=role['Role']['Arn']
            Environment=dict(Variables=dict())
            MemorySize=size
            )



def command_dict_to_file(command_dict):
    for key, no in zip(command_dict.keys(), range(len(command_dict.keys()))):
        lst = command_dict[key]

        with open("machine_%d.sh" % no, "w") as f:
            for cmd in lst:
                f.write(cmd + "\n\n")
