import hashlib
import random
import boto3


lc = boto3.client('lambda')
iam_client = boto3.client('iam')

# Generates a random RGB color
def get_random_color():
    def rand_256(): return random.randint(0, 255)
    return 'rgb(%d, %d, %d)' % (rand_256(), rand_256(), rand_256())

def get_all_lambdas():
    return lc.list_functions()['Functions']


def lambda_exists(existing, name, size, zip_location):

    # TODO: Check to see if uploaded SHA256 matches current bundle's SHA256
    # Code below doesn't work, not sure if I need to hash zip or undlerlying code... 
    # with open(zip_location, 'rb') as f:
    #    zipped_code = f.read()
    #bundle_sha = hashlib.sha256(zipped_code).hexdigest()

    def check(lambda_):
        return lambda_['FunctionName'] == name
       
    for lambda_ in existing:
        if (check(lambda_)):
            return True
    return False

def create_lambda(fname, size=128):
    with open(fname, 'rb') as f:
        zipped_code = f.read()
    
    role = iam_client.get_role(RoleName="fix_lambda_role")

    fn = "testfunc1_%d" % size

    lc.create_function(
            FunctionName=fn,
            Runtime="python2.7",
            Handler='handler.handler',
            Code=dict(ZipFile=zipped_code),
            Timeout=300,
            Role=role['Role']['Arn'],
            Environment=dict(Variables=dict()),
            VpcConfig = {
                'SubnetIds': ['subnet-bdb37ef4', 'subnet-db812abc', 'subnet-10082048'], 
                'SecurityGroupIds': ['sg-63cfa618', 'sg-8bfd6af1', 'sg-36138a4e']},
            MemorySize=size
            )



# Takes a dictionary in the form of { 'machine-public-ip': ['list of commands'] }
# and creates a bash file for each machine that will run the command list
def command_dict_to_file(command_dict):
    for key, no in zip(command_dict.keys(), range(len(command_dict.keys()))):
        lst = command_dict[key]

        with open("machine_%d.sh" % no, "w") as f:
            for cmd in lst:
                f.write(cmd + "\n\n")
