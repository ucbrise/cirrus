import os
import configparser
import logging

# The path at which Cirrus stores its configuration. Must be expanded with
#   os.path.expanduser.
CONFIGURATION_PATH = "~/.cirrus.cfg"

# The path to Cirrus' setup script, relative to the repository root.
SETUP_SCRIPT_PATH = "/python/frontend/cirrus/cirrus/setup.py"

cached_config = None


def config(check_exists=True):
    global cached_config
    log = logging.getLogger("cirrus.configuration.config")

    if cached_config is None:
        cached_config = configparser.ConfigParser()
        path = os.path.expanduser(CONFIGURATION_PATH)

        if os.path.exists(path):
            log.debug("Configuration loaded.")
            cached_config.read([path])
        elif check_exists:
            raise RuntimeError("Cirrus has not been configured. Run Cirrus' "
                               "setup script at %s before attempting to use "
                               "Cirrus." % SETUP_SCRIPT_PATH)
        else:
            log.debug("No configuration found. Ignoring.")

    return cached_config
