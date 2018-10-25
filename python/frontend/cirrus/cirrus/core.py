# Core model
import threading
import time
from abc import ABCMeta, abstractmethod

import boto3

import messenger
from CostModel import CostModel
import automate
import setup


# Code shared by all Cirrus experiments
# Contains all data for a single experiment
class BaseTask(object):
    __metaclass__ = ABCMeta

    def __init__(self,
            n_workers,
            lambda_size,
            n_ps,
            dataset,
            learning_rate,
            epsilon,
            ps,
            opt_method, # adagrad, sgd, nesterov, momentum
            checkpoint_model, # checkpoint model every x seconds
            train_set,
            test_set,
            minibatch_size,
            model_bits,
            use_grad_threshold,
            grad_threshold,
            timeout,
            threshold_loss,
            progress_callback
            ):
        self.thread = threading.Thread(target=self.run)
        self.n_workers = n_workers
        self.lambda_size = lambda_size
        self.n_ps = n_ps
        self.dataset=dataset
        self.learning_rate = learning_rate
        self.epsilon = epsilon
        self.ps = ps
        self.opt_method = opt_method
        self.checkpoint_model = checkpoint_model
        self.train_set=train_set
        self.test_set=test_set
        self.minibatch_size=minibatch_size
        self.model_bits=model_bits
        self.use_grad_threshold=use_grad_threshold
        self.grad_threshold=grad_threshold
        self.timeout=timeout
        self.threshold_loss=threshold_loss
        self.progress_callback=progress_callback
        self.dead = False
        self.cost_model = None
        self.total_cost = 0
        self.cost_per_second = 0
        self.total_lambda = 0
        self.id = 0
        self.kill_signal = threading.Event()
        self.num_lambdas = 0
        self.cost_model = CostModel(
                    'm5.large',
                    self.n_ps,
                    0,
                    self.n_workers,
                    self.lambda_size)

        self.time_cps_lst = []
        self.time_ups_lst = []
        self.time_loss_lst = []
        self.real_time_loss_lst = []
        self.start_time = time.time()

        # Signals that the experiment should be stopped. See `run` and `kill`.
        self.stop_event = threading.Event()

        # Stored values
        self.last_num_lambdas = 0

    def get_name(self):
        string = "Rate %f" % self.learning_rate
        return string

    def get_cost_per_second(self):
        
        elapsed = time.time() - self.start_time
        cps = self.cost_model.get_cost_per_second()
        self.time_cps_lst.append((time.time() - self.start_time, cps))
        return self.time_cps_lst

    def get_num_lambdas(self, fetch=True):
        if self.is_dead():
            return 0
        if fetch:
            out = messenger.get_num_lambdas(self.ps)
            if out is not None:
                self.last_num_lambdas = out
            return self.last_num_lambdas
        else:
            return self.num_lambdas

    def get_updates_per_second(self, fetch=True):
        if self.is_dead():
            return self.time_ups_lst
        if fetch:
            t = time.time() - self.start_time
            ups = messenger.get_num_updates(self.ps)
            self.time_ups_lst.append((t, ups))
            return self.time_ups_lst
        else:
            return self.time_ups_lst


    def get_time_loss(self, rtl=False):

        if self.is_dead():
            if rtl:
                return self.real_time_loss_lst
            else:
                return self.time_loss_lst
        out = messenger.get_last_time_error(self.ps)
        if out == None:
            if rtl:
                return self.real_time_loss_lst
            else:
                return self.time_loss_lst
        t, loss, real_time_loss = out

        if t == 0:
            if rtl:
                return self.real_time_loss_lst
            else:
                return self.time_loss_lst

        if len(self.time_loss_lst) == 0 or not ((t, loss) == self.time_loss_lst[-1]):
            self.time_loss_lst.append((t, loss))
        self.real_time_loss_lst.append((time.time() - self.start_time, real_time_loss))
        if rtl:
            return self.real_time_loss_lst
        else:
            return self.time_loss_lst

    def run(self):
        """Run this task.

        Starts a parameter server and launches a fleet of workers.
        """
        self.ps.start(self.define_config())
        self.stop_event.clear()

        time.sleep(10) # give some time to parameter server

        automate.maintain_workers(self.n_workers, setup.LAMBDA_NAME,
            self.define_config(), self.ps, self.stop_event)


    def kill(self):
        """Kill this task.

        Stops the parameter server and the fleet of workers.
        """
        # The order of these is significant. By stopping the parameter server
        #   first, we ensure that the remaining workers will error when they try
        #    to contact the parameter server, and so exit in a short amount of
        #    time.
        self.ps.stop()
        self.stop_event.set()

    def is_dead(self):
        return self.dead


    @abstractmethod
    def define_config(self, fetch=False):
        pass
