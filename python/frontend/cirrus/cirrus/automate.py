import argparse

BUILD_IMAGE_NAME = "cirrus_build_image"
SERVER_IMAGE_NAME = "cirrus_server_image"
EXECUTABLES_PATH = "s3://cirrus/executables"
LAMBDA_PACKAGE_PATH = "s3://cirrus_automate/lambda_package"
LAMBDA_NAME = "cirrus_worker_lambda"


class Instance(object):
	"""AN EC2 instance."""

	def __init__(self, ...):
		pass

	def start(self):
		pass
	
	def __enter__(self):
		pass

	def ssh(self):
		pass

	def sftp(self):
		pass

	def terminate(self):
		pass

	def __exit__(self):
		pass


def make_build_image(name, replace=False):
	"""Make an AMI sutiable for building the backend on.

	Args:
		name (str): The name to give the AMI.
		replace (bool): Whether to replace any existing AMI with the same name.
			If False or omitted, and an AMI with the same name exists, will do
			nothing.
	"""
	pass


def make_executables(path, instance):
	"""Build the backend and publish its executables.

	Args:
		path (str): A S3 path to a "directory" in which to publish the
			executables.
		instance (EC2Instance): The instance on which to build. Should be a
			fresh launch of the AMI produced by `make_build_image`.
	"""
	pass


def make_lambda_package(path, executables_path):
	"""Make and publish the ZIP package for the worker Lambda function.

	Args:
		path (str): An S3 path at which to publish the package.
		executables_path (str): An S3 path to a "directory" from which to get
			the parameter server executable.
	"""
	pass


def make_server_image(name, executables_path, instance):
	"""Make an AMI that runs parameter servers.

	Args:
		name (str): The name to give the AMI.
		executables_path (str): An S3 path to a "directory" from which to get
			the parameter server executable.
		instance (EC2Instance): The instance to use to set up the image. Should
			be a fresh launch of the AMI produced by `make_build_image`.
	"""
	pass


def make_lambda(name, lambda_package_path):
	"""Make a worker Lambda function.

	Args:
		name (str): The name to give to the Lambda.
		lambda_package_path (str): An S3 path to the Lambda ZIP package.
	"""
	pass


def launch_worker(lambda_name):
	"""Launch a worker.

	Args:
		lambda_name (str): The name of the worker Lambda function.
	"""
	pass


def launch_server(server_image_name):
	"""Launch a parameter server.

	Args:
		server_image_name (str): The AMI to use for the server. Should be an AMI
			produced by `make_server_image`.
	"""
	pass


def build():
	"""Build Cirrus. Publishes backend executables, a Lambda ZIP package, and a
		parameter server AMI.
	"""
	make_build_image(BUILD_IMAGE_NAME)
	with Instance(BUILD_IMAGE_NAME) as instance:
		make_executables(EXECUTABLES_PATH, instance)
		make_lambda_package(LAMBDA_PACKAGE_PATH, EXECUTABLES_PATH)
		make_server_image(SERVER_IMAGE_NAME, EXECUTABLES_PATH, instance)


def deploy():
	"""Deploy Cirrus. Creates a worker Lambda.
	"""
	make_lambda(LAMBDA_NAME, LAMBDA_PACKAGE_PATH)
