import time

import pytest

from .context import cirrus


@pytest.mark.timeout(10)
def test_foo():
    try:
        def progress_callback(time_loss, cost, task):
            print("Current training loss:", time_loss, \
                "current cost ($): ", cost)

        data_bucket = 'cirrus-criteo-kaggle-19b-random'
        model = 'model_v1'

        cf_task = cirrus.CollaborativeFiltering(
                     # number of workers
                     n_workers = 5,
                     # number of parameter servers
                     n_ps = 2,
                     # worker size in MB
                     worker_size = 128,
                     # path to s3 bucket with input dataset
                     dataset = data_bucket,
                     # sgd update LR and epsilon
                     learning_rate=0.01,
                     epsilon=0.0001,
                     progress_callback = progress_callback,
                     # stop workload after these many seconds
                     timeout = 0,
                     # stop workload once we reach this loss
                     threshold_loss=0,
                     # resume execution from model stored in this s3 bucket
                     resume_model = model,
                     # aws key name
                     key_name='mykey',
                     # path to aws key
                     key_path='/home/camus/Downloads/mykey.pem',
                     # ip where ps lives
                     ps_ip_public='ec2-54-188-0-164.us-west-2.compute.amazonaws.com',
                     ps_ip_private='172.31.26.54',
                     # username of VM
                     ps_username='ubuntu',
                     # choose between adagrad, sgd, nesterov, momentum
                     opt_method = 'adagrad',
                     # checkpoint model every x secs
                     checkpoint_model = 60,
                     #
                     minibatch_size=20,
                     # model size
                     model_bits=19,
                     # whether to filter gradient weights
                     use_grad_threshold=False,
                     # threshold value
                     grad_threshold=0.001,
                     # range of training minibatches
                     train_set=(0,824),
                     # range of testing minibatches
                     test_set=(835,840)
                     )
        cf_task.run_cirrus()
        time.sleep(10)
        cf_task.kill()
        assert True
    except:
        assert False
