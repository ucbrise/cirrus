ps aux | grep parameter_server | awk '{print $2}' | xargs kill -9
