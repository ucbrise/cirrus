import logging
import pipes
import time
import socket

from . import handler


class ParameterServer(object):
    """A parameter server and its associated error task.
    """

    # The maximum amount of time that a parameter server can take to start, in
    #   seconds.
    MAX_START_TIME = 60

    # The number of additional connections needed in order for messenger.py to
    # communicate with the parameter server.
    ADDITIONAL_CONNS = 5

    def __init__(self, instance, ps_port, error_port, num_workers):
        """Create a parameter server.

        Args:
            instance (instance.Instance): The instance on which this parameter
                server should be run.
            ps_port (int): The port that the parameter server should listen on.
            error_port (int): The port that the error task should listen on.
            num_workers (int): A value for the parameter server's `nworkers`
                command-line argument.
        """
        # TODO: Figure out exactly what "nworkers" is used for.
        self._instance = instance
        self._ps_port = ps_port
        self._error_port = error_port
        self._num_workers = num_workers

        self._log = logging.getLogger("cirrus.automate.ParameterServer")

    def public_ip(self):
        """Get the public IP address which the parameter server is accessible
            from.

        Returns:
            str: The IP address.
        """
        return self._instance.public_ip()

    def private_ip(self):
        """Get the private IP address which the parameter server is accessible
            from.

        Returns:
            str: The IP address.
        """
        return self._instance.private_ip()

    def ps_port(self):
        """Get the port number which the parameter server is accessible from.

        Returns:
            int: The port.
        """
        return self._ps_port

    def error_port(self):
        """Get the port number which the error task is accessible from.

        Returns:
            int: The port.
        """
        return self._error_port

    def start(self, config):
        """Start this parameter server and its associated error task.

        Does not block until the parameter server has finished startup. See
            `wait_until_started` for that.

        Args:
            config (str): The contents of a parameter server configuration file.
        """
        self._log.debug("Uploading configuration.")
        config_filename = "config_%d.txt" % self._ps_port
        self._instance.run_command(
            "echo %s > %s" % (pipes.quote(config), config_filename))

        self._log.debug("Starting parameter server.")
        ps_start_command = " ".join((
            "ulimit -c unlimited;",
            "nohup",
            "./parameter_server",
            "--config", config_filename,
            "--nworkers", str(self._num_workers + ADDITIONAL_CONNS),
            "--rank", "1",
            "--ps_port", str(self._ps_port),
            "&>", "ps_out_%d" % self._ps_port,
            "&",
            "echo $! > ps_%d.pid" % self._ps_port
        ))
        status, _, stderr = self._instance.run_command(ps_start_command)
        if status != 0:
            print("An error occurred while starting the parameter server."
                  " The exit code was %d and the stderr was:" % status)
            print(stderr)
            raise RuntimeError("An error occurred while starting the parameter"
                               " server.")

        self._log.debug("Starting error task.")
        error_start_command = " ".join((
            "ulimit -c unlimited;",
            "nohup",
            "./parameter_server",
            "--config", config_filename,
            "--nworkers", str(self._num_workers),
            "--rank 2",
            "--ps_ip", self._instance.private_ip(),
            "--ps_port", str(self.ps_port()),
            "&> error_out_%d" % self.ps_port(),
            "&",
            "echo $! > error_%d.pid" % self.ps_port()
        ))
        status, _, stderr = self._instance.run_command(error_start_command)
        if status != 0:
            print("An error occurred while starting the error task."
                  " The exit code was %d and the stderr was:" % status)
            print(stderr)
            raise RuntimeError("An error occurred while starting the error"
                               " task.")

    def wait_until_started(self):
        """Block until this parameter server has started.

        The parameter server is considered to have started once attempts to
            connect to it begin succeeding.

        Raises:
            RuntimeError: If this parameter server takes too long to start (or,
                presumably, crashes).
        """
        total_attempts = self.MAX_START_TIME // handler.PS_CONNECTION_TIMEOUT

        for attempt in range(1, total_attempts + 1):
            self._log.debug("Making connection attempt #%d to %s."
                            % (attempt, self))
            start = time.time()
            if self.reachable():
                self._log.debug("%s launched." % self)
                return
            elapsed = time.time() - start

            remaining = handler.PS_CONNECTION_TIMEOUT - elapsed
            if remaining > 0:
                time.sleep(remaining)

        raise RuntimeError("%s appears not to have started successfully."
                           % self)

    def stop(self):
        """Stop this parameter server and its associated error task.

        Sends SIGKILL to both processes.
        """
        for task in ("error", "ps"):
            kill_command = "kill -n 9 $(cat %s_%d.pid)" % (task, self.ps_port())
            _, _, _ = self._instance.run_command(kill_command)
            # TODO: Here we should probably wait for the process to die and
            #   raise an error if it doesn't in a certain amount of time.

    def ps_output(self):
        """Get the output of this parameter server so far.

        Combines the parameter server's stdout and stderr.

        Returns:
            str: The output.
        """
        command = "cat ps_out_%d" % self.ps_port()
        status, stdout, stderr = self._instance.run_command(command)
        if status != 0:
            print("An error occurred while getting the output of the parameter "
                  "server. The exit code was %d and the stderr was:" % status)
            print(stderr)
            raise RuntimeError("An error occurred while getting the output of "
                               " the parameter server.")
        return stdout

    def error_output(self):
        """Get the output of the error task so far.

        Combines the error task's stdout and stderr.

        Returns:
            str: The output.
        """
        command = "cat error_out_%d" % self.ps_port()
        status, stdout, stderr = self._instance.run_command(command)
        if status != 0:
            print("An error occurred while getting the output of the error "
                  "task. The exit code was %d and the stderr was:" % status)
            print(stderr)
            raise RuntimeError("An error occurred while getting the output of "
                               " the error task.")
        return stdout

    def reachable(self):
        """Return whether this parameter server is reachable.

        This parameter server is reachable if attempts to connect to it succeed.

        Returns:
            bool: Whether this parameter server is reachable.
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(handler.PS_CONNECTION_TIMEOUT)
            sock.connect((self.public_ip(), self.ps_port()))
            sock.close()
            return True
        except:
            return False

    def __str__(self):
        """Return a string representation of this parameter server.

        Returns:
            str: The string representation.
        """
        return "ParameterServer@%s:%d" % (self.public_ip(), self.ps_port())
