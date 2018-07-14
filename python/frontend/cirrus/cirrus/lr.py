# Logistic Regression

import threading
import paramiko
import time
import os
import boto3
from threading import Thread
from CostModel import CostModel
from core import BaseTask

class LogisticRegressionTask(BaseTask):
    def __init__(self, *args, **kwargs):
        # pass all arguments of init to parent class
        super(LogisticRegressionTask, self).__init__(*args, **kwargs)

    def __del__(self):
        print("Logistic Regression Task Lost")


    def define_config(self, ip):
        if self.use_grad_threshold:
            grad_t = 1
        else:
            grad_t = 0

        config = "input_path: /mnt/efs/criteo_kaggle/train.csv \n" + \
                 "input_type: csv\n" + \
                 "num_classes: 2 \n" + \
                 "num_features: 13 \n" + \
                 "limit_cols: 14 \n" + \
                 "normalize: 1 \n" + \
                 "limit_samples: 50000000 \n" + \
                 "s3_size: 50000 \n" + \
                 "use_bias: 1 \n" + \
                 "model_type: LogisticRegression \n" + \
                 "minibatch_size: %d \n" % self.minibatch_size + \
                 "learning_rate: %f \n" % self.learning_rate + \
                 "epsilon: %lf \n" % self.epsilon + \
                 "model_bits: %d \n" % self.model_bits + \
                 "s3_bucket: %s \n" % self.dataset + \
                 "use_grad_threshold: %d \n" % grad_t + \
                 "grad_threshold: %lf \n" % self.grad_threshold + \
                 "train_set: %d-%d \n" % self.train_set + \
                 "test_set: %d-%d" % self.test_set

        key = paramiko.RSAKey.from_private_key_file(self.key_path)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip, username=self.ps_username, pkey=key)
        stdin, stdout, stderr = client.exec_command('echo "%s" > config.txt' % config)
        client.close()

def LogisticRegression(
            n_workers,
            n_ps,
            worker_size,
            dataset,
            learning_rate, epsilon,
            progress_callback,
            resume_model,
            key_name,
            key_path,
            train_set,
            test_set,
            minibatch_size,
            model_bits,
            ps_ip_public="",
            ps_ip_private="",
            ps_ip_port=1337,
            ps_username="ec2-user",
            opt_method="sgd",
            checkpoint_model=0,
            use_grad_threshold=False,
            grad_threshold=0.001,
            timeout=60,
            threshold_loss=0
            ):
    return LogisticRegressionTask(
            n_workers=n_workers,
            n_ps=n_ps,
            worker_size=worker_size,
            dataset=dataset,
            learning_rate=learning_rate,
            epsilon=epsilon,
            key_name=key_name,
            key_path=key_path,
            ps_ip_public=ps_ip_public,
            ps_ip_private=ps_ip_private,
            ps_ip_port=ps_ip_port,
            ps_username=ps_username,
            opt_method=opt_method,
            checkpoint_model=checkpoint_model,
            train_set=train_set,
            test_set=test_set,
            minibatch_size=minibatch_size,
            model_bits=model_bits,
            use_grad_threshold=use_grad_threshold,
            grad_threshold=grad_threshold,
            timeout=timeout,
            threshold_loss=threshold_loss,
            progress_callback=progress_callback
           )

def create_random_lr_model(n):
    #print "Creating generic model not yet implemented, creating criteo kaggle model"
    print "Creating random LR model with size: ", n

    return 0

# Collaborative Filtering
def CollaborativeFiltering():
    print "not implemented"


# LDA algorithm
def LDA():
    print "Not implemented"
