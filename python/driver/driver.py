# this is the driver program that orchestrates the
# ML workload in the backend

# general arguments for driver
# 1. workload

# specific args for logistic regression
# 1. number of workers
# 2. number of PSs
# 3. path to dataset
# 4. learning_rate
# 5. epsilon


def launch_error_task():
    print "Launching the error task"

def launch_parameter_server():
    print "Launching the parameter server"

def start_lambdas():
    print "Starting lambdas"

def serve_rpcs():
    print "Waiting for RPCs from user frontend"

def print_args():
    print "Driver received these arguments:"
    for arg in sys.argv:
        print arg

if __name__ == "main":
    print_args()
    args = parse_args()

    # launch parameter servers
    # here we assume:
    # only one PS supported
    # PS binary has been transferred here

    launch_parameter_server()

    # launch error task
    launch_error_task()

    # start_lambdas
    start_lambdas()

    # serve RPCs from the user frontend
    serve_rpcs()

