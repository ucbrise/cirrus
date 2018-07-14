

import time
import random
from context import cirrus
from cirrus import LogisticRegression
from cirrus import app
from cirrus import CirrusBundle

def progress_callback(time_loss, cost, task):
    pass
    #print("Current training loss:", time_loss, "current cost ($): ", cost)

url = "ec2-34-212-6-172.us-west-2.compute.amazonaws.com"
ip = "172.31.5.74"

ps_servers = [
    (url, ip, 0.1),
    (url, ip, 0.1),
    (url, ip, 0.1),
    (url, ip, 0.1),
]

data_bucket = 'cirrus-criteo-kaggle-19b-random'
model = 'model_v1'

basic_params = {
    'n_workers': 3,
    'n_ps': 2,
    'worker_size': 128,
    'dataset': data_bucket,
    'learning_rate': 0.01,
    'epsilon': 0.0001,
    'progress_callback': None,
    'timeout': 0,
    'threshold_loss': 0,
    'resume_model': model,
    'key_name': 'mykey',
    'key_path': '/home/camus/Downloads/mykey.pem',
    'ps_ip_public': 'ec2-54-71-177-228.us-west-2.compute.amazonaws.com',
    'ps_ip_private': '172.31.9.205',
    'ps_username': 'ubuntu',
    'opt_method': 'adagrad',
    'checkpoint_model': 60,
    'minibatch_size': 20,
    'model_bits': 19,
    'use_grad_threshold': False,
    'grad_threshold': 0.001,
    'train_set': (0,824),
    'test_set': (835,840)
}

if __name__ == "__main__":
    batch = []
    index = 0
    base_port = 1337
    for ps in ps_servers:
        config = basic_params.copy()
        config['ps_ip_public'] = ps[0]
        config['ps_ip_private'] = ps[1]
        config['ps_ip_port'] = base_port + (index * 2)
        config['learning_rate'] = ps[2]
        batch.append(config)
        index += 1

    cb = CirrusBundle()
    cb.set_task_parameters(LogisticRegression, batch)
    cb.set_jobs(2)
    cb.run_bundle()
    print("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
    while True:
        time.sleep(1)
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx")
        print(cb.get_top(-5))
