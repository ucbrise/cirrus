import os
import subprocess
import sys
import logging
import time
import socket
import struct

# The path at which to save the configuration in a worker Lambda.
CONFIG_PATH = "/tmp/config.cfg"

# The name of the worker executable.
EXECUTABLE_NAME = "parameter_server"

# The interval at which to check for the worker process' exit, in seconds.
EXIT_POLL_INTERVAL = 0.01

# A message to the parameter server that requests worker registration.
REGISTER_TASK_MSG = b'\x08\x00\x00\x00'

# The timeout for an attempt to connect to a parameter server, in seconds.
PS_CONNECTION_TIMEOUT = 5

# The size of buffer to use for relaying the worker's output to the Lambda's
#   output, in bytes.
WORKER_OUTPUT_BUFFER = 100


def register_task_id(ps_ip, ps_port, task_id, time_left):
    """Attempt to register a worker with a parameter server.

    Args:
        ps_ip (str): The public IP address of the parameter server.
        ps_port (int): The port of the parameter server.
        task_id (int): The ID number of the task this worker is running.
        time_left (func[] -> int): A function that takes no arguments and
            returns the maximum amount of time the worker will live, in
            milliseconds, starting from the moment of the call.

    Returns:
        bool: Whether registration succeeded, according to the parameter server.
    """
    # Set up the connection.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(PS_CONNECTION_TIMEOUT)
    sock.connect((ps_ip, ps_port))

    # Request registration.
    sock.send(REGISTER_TASK_MSG)
    time_left_secs = time_left() // 1000
    sock.send(struct.pack("II", task_id, time_left_secs))

    # Check result.
    response = sock.recv(32)
    sock.close()
    result = struct.unpack("I", response)[0]
    # A result of 0 indicates success.
    return result == 0


def run(event, context):
    """Respond to a request that a worker be run.

    Args:
        event (dict[str, *]): The request. The value for `"log_level"` gives
            the name of the level of logs to write to CloudWatch. The value for
            `"task_id"` gives the task ID number to be assigned to the worker.
            The values for `"ps_ip"` and `"ps_port"` give the public IP address
            and port of the parameter server that the worker should interface
            with.
        context (dict[str, *]): Information about the current execution context.

    Returns:
        dict[str, *]: The response.
    """
    # TODO: Document num_workers and config keys.

    level = getattr(logging, event["log_level"])
    logging.getLogger().setLevel(level)
    # It appears that Amazon attaches a handler to the root logger that sends
    #   logs to CloudWatch. It is still necessary to set a proper level though.
    log = logging.getLogger("cirrus.handler.run")

    # Attempt to catch any errors that arise, to enable better logging.
    try:
        log.debug("This is an invocation of %s, version %s."
                  % (context.function_name, context.function_version))
        log.debug("Logging to stream `%s`." % context.log_stream_name)
        log.debug("Logging to group `%s`." % context.log_group_name)
        log.debug("The request ID is %s." % context.aws_request_id)
        log.debug("The memory limit is %sMB." % context.memory_limit_in_mb)
        log.debug("The time remaining is %dms."
                  % context.get_remaining_time_in_millis())

        task_id = event["task_id"]
        num_workers = event["num_workers"]
        ps_ip = event["ps_ip"]
        ps_port = event["ps_port"]

        log.debug("This is Task %d, interfacing with %s:%d."
                  % (task_id, ps_ip, ps_port))

        # Attempt to register with the parameter server; abort if a duplicate
        #   invocation with the same worker ID has already won the race.
        log.debug("Attempting registration.")
        registration_succeeded = register_task_id(
            ps_ip,
            ps_port,
            task_id,
            context.get_remaining_time_in_millis
        )
        if not registration_succeeded:
            log.info("Terminating due to registration failure.")
            return {
                "statusCode": 200,
                "body": "Registration failure."
            }
        else:
            log.debug("Registered successfully.")

        # Write the configuration provided to a file.
        with open(CONFIG_PATH, "w+") as config_file:
            config_file.write(event["config"])
        log.debug("Wrote config.")

        # Run the worker, relaying its output to our output.
        command = [
            os.path.join(os.environ["LAMBDA_TASK_ROOT"], EXECUTABLE_NAME),
            "--config", CONFIG_PATH,
            "--nworkers", str(num_workers),
            "--rank", str(3),
            "--ps_ip", ps_ip,
            "--ps_port", str(ps_port)
        ]
        log.debug("Starting worker with command `%s`." % " ".join(command))
        process = subprocess.Popen(command, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        for buf in iter(lambda: process.stdout.read(WORKER_OUTPUT_BUFFER), b''):
            sys.stdout.write(buf)

        # Wait for the worker process to exit.
        while process.poll() is None:
            time.sleep(EXIT_POLL_INTERVAL)

        # Check the exit code of the worker process.
        if process.returncode >= 0:
            msg = "The worker exited with code %d." % process.returncode
        else:
            msg = "The worker died with signal %d." % (-process.returncode)
        if process.returncode == 0:
            log.debug(msg)
        else:
            log.error(msg)
            raise RuntimeError(msg)

        return {
            "statusCode": 200,
            "body": "Success."
        }
    except:
        log.error("The handler threw an error.")
        raise
