"""Utilities for setting up an installation of Cirrus."""

import textwrap
import os
import configparser

import boto3
import botocore.exceptions


def run_interactive_setup():
    """Run an interactive command-line setup process.
    """
    config = configparser.ConfigParser()
    config["aws"] = {}

    if not _aws_authorized():
        _setup_aws_credentials()

    _setup_region(config)

    _save_config(config)


def _setup_aws_credentials():
    EXPLANATION = textwrap.dedent("""\
        Please enter the ID of one of your AWS access keys. This will enable
            Cirrus to create AWS resources on your behalf. See
            https://amzn.to/2CagUqm for how to retrieve this information.""")
    PROMPTS = ("Access key ID", "Secret access key")

    def validator(id, secret):
        session = boto3.session.Session(id, secret)
        ec2 = session.client("ec2", "us-west-1")
        return _aws_authorized(id, secret)

    id, secret = prompt(EXPLANATION, PROMPTS, validator)

    EXPLANATION = textwrap.dedent("""\
        May Cirrus write your AWS credentials to ~/.aws/credentials?""")
    PROMPTS = ("y/n",)
    validator = lambda c: c  in ("y", "n")
    postprocessor = lambda c: c == "y"

    can_write = prompt(EXPLANATION, PROMPTS, validator, postprocessor)
    if not can_write:
        print("Please set up your AWS credentials manually, so that they can "
              "be read by boto3.")
        return

    credentials = textwrap.dedent(f"""\
        [default]
        aws_access_key_id = {id}
        aws_secret_access_key = {secret}""")
    os.makedirs(os.path.expanduser("~/.aws"), exist_ok=True)
    with open(os.path.expanduser("~/.aws/credentials"), "w+") as f:
        f.write(credentials)


def _aws_authorized(id=None, secret=None):
    session = boto3.session.Session(id, secret)
    ec2 = session.client("ec2", "us-west-1")
    try:
        # We're doing this to test out the account information. The only way to
        #   check it is to try using it to sign a request.
        ec2.describe_regions()
    except botocore.exceptions.ClientError as e:
        if e.args[0].startswith("An error occurred (AuthFailure)"):
            return False
        else:
            raise e
    else:
        return True


def _setup_region(config):
    EXPLANATION = "What AWS region do you want Cirrus to use?"
    PROMPTS = ("Region",)
    regions = boto3.session.Session().get_available_regions("lambda")
    validator = lambda region: region in regions

    region = prompt(EXPLANATION, PROMPTS, validator)

    config["aws"]["region"] = region


def _save_config(config):
    with open(os.path.expanduser("~/.cirrus.cfg"), "w+") as f:
        config.write(f)


def prompt(explanation, prompts, validator=None, postprocess=None):
    if validator is None:
        def validator(*args):
            return True

    if postprocess is None:
        def postprocess(*args):
            if len(args) == 1:
                return args[0]
            return args

    print()
    print()
    print(explanation)

    while True:
        values = [input(prompt + ": ") for prompt in prompts]
        if validator(*values):
            break
        else:
            print()
            print("Invalid.")

    return postprocess(*values)


if __name__ == "__main__":
    run_interactive_setup()
