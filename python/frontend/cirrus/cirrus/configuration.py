import os
import configparser

# The path at which Cirrus stores its configuration. Must be expanded with
#   os.path.expanduser.
CONFIGURATION_PATH = "~/.cirrus.cfg"

config = configparser.ConfigParser()
path = os.path.expanduser(CONFIGURATION_PATH)
if os.path.exists(path):
    config.read([path])
