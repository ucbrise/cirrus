"""Tests that the Lambda request handler works as expected, at least in the
    local environment.
"""
import textwrap
import os
import tempfile
import logging
import sys

from cirrus import handler

PS_IP = "127.0.0.1"
PS_PORT = 1543


def test_registration_success():
    """Test that the request handler runs when registration succeeds.
    """
    assert _run_test(True)["body"] == "Success."


def test_registration_failure():
    """Test that the request handler exits when registration fails.
    """
    assert _run_test(False)["body"] == "Registration failure."


def _run_test(registration_result):
    old_config_path = handler.CONFIG_PATH
    old_register_task_id = handler.register_task_id
    old_executable_name = handler.EXECUTABLE_NAME
    old_task_root = os.environ.get("LAMBDA_TASK_ROOT")

    def register(*args):
        return registration_result

    handler.register_task_id = register
    with tempfile.NamedTemporaryFile() as config_file:
        with tempfile.NamedTemporaryFile() as executable:
            with tempfile.NamedTemporaryFile() as flag:
                handler.CONFIG_PATH = config_file.name
                handler.EXECUTABLE_NAME = os.path.basename(executable.name)
                os.environ["LAMBDA_TASK_ROOT"] = os.path.dirname(executable.name)


                # TODO: This doesn't work unless using "sh".
                executable.write(textwrap.dedent("""
                    #!/usr/bin/env python2
                    import time
                    time.sleep(2)"""
                ))
                os.chmod(executable.name, 0x777)

                event = {
                    "log_level": "DEBUG",
                    "worker_id": 5,
                    "ps_ip": PS_IP,
                    "ps_port": PS_PORT,
                    "num_workers": 10,
                    "config": "config bla"
                }

                class Context:
                    function_name = "foo"
                    function_version = "2"
                    log_stream_name = "bla stream"
                    log_group_name = "bla group"
                    aws_request_id = "request bla"
                    memory_limit_in_mb = "512"

                    def get_remaining_time_in_millis(self):
                        return 10000

                result = handler.run(event, Context())

    handler.CONFIG_PATH = old_config_path
    handler.register_task_id = old_register_task_id
    handler.EXECUTABLE_NAME = old_executable_name
    if old_task_root is not None:
        os.environ["LAMBDA_TASK_ROOT"] = old_task_root
    return result


if __name__ == "__main__":
    log = logging.getLogger("cirrus")
    log.setLevel(logging.DEBUG)
    log.addHandler(logging.StreamHandler(sys.stdout))

    print("===== REGISTRATION SUCCESS =====")
    test_registration_success()

    print("")
    print("===== REGISTRATION FAILURE =====")
    test_registration_failure()
