# Core model
import threading
import time
from abc import ABCMeta, abstractmethod

import boto3

import messenger
from CostModel import CostModel

lambda_client = boto3.client('lambda', 'us-west-2')
lambda_name = "testfunc1"


# Code shared by all Cirrus experiments
# Contains all data for a single experiment
class BaseTask(object):
    __metaclass__ = ABCMeta

    # Keys for metrics
    COST_PER_SECOND = "cost_per_second"
    UPDATES_PER_SECOND = "updates_per_second"
    LOSS_VS_TIME = "loss_vs_time"
    REAL_TIME_LOSS_VS_TIME = "real_time_loss_vs_time"
    TOTAL_LOSS_VS_TIME = "total_loss_vs_time"


    def __init__(self,
            n_workers,
            lambda_size,
            n_ps,
            dataset,
            learning_rate,
            epsilon,
            key_name, key_path, # aws key
            ps_ip_public, # public parameter server ip
            ps_ip_private, # private parameter server ip
            ps_ip_port,
            ps_username, # parameter server VM username
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
        self.n_ps = n_ps
        self.dataset=dataset
        self.learning_rate = learning_rate
        self.epsilon = epsilon
        self.key_name = key_name
        self.key_path = key_path
        self.ps_ip_public = ps_ip_public
        self.ps_ip_private = ps_ip_private
        self.ps_ip_port = ps_ip_port
        self.ps_username = ps_username
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

        self.start_time = time.time()

        self.metrics = {
            self.COST_PER_SECOND: [],
            self.UPDATES_PER_SECOND: [],
            self.LOSS_VS_TIME: [],
            self.REAL_TIME_LOSS_VS_TIME: [],
            self.TOTAL_LOSS_VS_TIME: []
        }


        # Stored values
        self.last_num_lambdas = 0

    def get_name(self):
        string = "Rate %f" % self.learning_rate
        return string


    def get_cost_per_second(self):
        return self.time_cps_lst

    def get_num_lambdas(self, fetch=True):
        if self.is_dead():
            return 0
        if fetch:
            out = messenger.get_num_lambdas(self.ps_ip_public, self.ps_ip_port)
            if out is not None:
                self.last_num_lambdas = out
            return self.last_num_lambdas
        else:
            return self.last_num_lambdas

    def get_updates_per_second(self, fetch=True):
        if self.is_dead():
            return self.time_ups_lst
        if fetch:
            t = time.time() - self.start_time
            ups = messenger.get_num_updates(self.ps_ip_public, self.ps_ip_port)
            self.time_ups_lst.append((t, ups))

            cost_per_second = self.cost_model.get_cost(t)
            if ups is not None and ups != 0:
                self.time_cps_lst.append((t, cost_per_second / ups))

            return self.time_ups_lst
        else:
            return self.time_ups_lst

        num_lambdas = self.get_num_lambdas()
        self.get_updates_per_second()
        num_task = 3

        if num_lambdas == None:
            return

        if num_lambdas < self.n_workers:
            shortage = self.n_workers - num_lambdas

            payload = '{"num_task": %d, "num_workers": %d, "ps_ip": \"%s\", "ps_port": %d}' \
                        % (num_task, self.n_workers, self.ps_ip_private, self.ps_ip_port)
            for i in range(shortage):
                try:
                    response = lambda_client.invoke(
                        FunctionName="%s_%d" % (lambda_name, self.worker_size),
                        InvocationType='Event',
                        LogType='Tail',
                        Payload=payload)
                except Exception as e:
                    print "client.invoke exception caught"
                    print str(e)

    def get_time_loss(self, rtl=False):

        if self.is_dead():
            if rtl:
                return self.real_time_loss_lst
            else:
                return self.time_loss_lst
        out = messenger.get_last_time_error(self.ps_ip_public, self.ps_ip_port + 1)
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
        self.define_config(self.ps_ip_public)
        self.launch_ps()
        self.relaunch_lambdas()

    def kill(self):
        messenger.send_kill_signal(self.ps_ip_public, self.ps_ip_port)
        self.kill_signal.set()
        self.dead = True

    def is_dead(self):
        return self.dead

    def get_command(self, command_dict):
        self.copy_config(command_dict)
        self.launch_ps(command_dict)
        self.launch_error_task(command_dict)

    def launch_error_task(self, command_dict=None):
        cmd = 'nohup ./parameter_server --config config_%d.txt --nworkers %d --rank 2 --ps_ip %s --ps_port %d &> error_out_%d &' % (
        self.ps_ip_port, self.n_workers, self.ps_ip_private, self.ps_ip_port, self.ps_ip_port)
        if command_dict is not None:
            command_dict[self.ps_ip_public].append(cmd)
        else:
            raise ValueError('SSH Error Task not implemented')

    def launch_ps(self, command_dict=None):
        cmd = 'nohup ./parameter_server --config config_%d.txt --nworkers %d --rank 1 --ps_port %d &> ps_out_%d & ' % (
            self.ps_ip_port, self.n_workers * 2, self.ps_ip_port, self.ps_ip_port)
        if command_dict is not None:
            command_dict[self.ps_ip_public].append(cmd)
        else:
            raise ValueError('SSH Copy config not implemented')

    def copy_config(self, command_dict=None):
        config = self.define_config()
        if command_dict is not None:
            command_dict[self.ps_ip_public].append('echo "%s" > config_%d.txt' % (config, self.ps_ip_port))
        else:
            raise ValueError('SSH Copy config not implemented')


    @abstractmethod
    def define_config(self, fetch=False):
        pass

    # This function is responsible for relaunching the lambdas as they die off. 
    # It will poll the parameter server to determine how many lambdas are currently running
    # and relaunch self.n_workers - num_lambdas number of lambdas
    def relaunch_lambdas(self):
        if self.is_dead():
            return

        num_lambdas = self.get_num_lambdas()
        self.get_updates_per_second()
        self.get_cost_per_second()
        num_task = 3

        if num_lambdas == None:
            return

        if num_lambdas < self.n_workers:
            shortage = self.n_workers - num_lambdas

            payload = '{"num_task": %d, "num_workers": %d, "ps_ip": \"%s\", "ps_port": %d}' \
                        % (num_task, self.n_workers, self.ps_ip_private, self.ps_ip_port)
            for i in range(shortage):
                try:
                    response = lambda_client.invoke(
                        FunctionName=lambda_name,
                        InvocationType='Event',
                        LogType='Tail',
                        Payload=payload)
                except Exception as e:
                    print "client.invoke exception caught"
                    print str(e)



    def get_cost_per_second(self):

        cps = self.cost_model.get_cost_per_second()
        self.fetch_metric(self.COST_PER_SECOND).append((time.time() - self.start_time, cps))
        return self.fetch_metric(self.COST_PER_SECOND)

    def get_num_lambdas(self, fetch=True):
        if self.is_dead():
            return 0
        if fetch:
            out = messenger.get_num_lambdas(self.ps_ip_public, self.ps_ip_port)
            if out is not None:
                self.last_num_lambdas = out
            return self.last_num_lambdas
        else:
            return self.last_num_lambdas

    def get_updates_per_second(self, fetch=True):
        if self.is_dead():
            return self.time_ups_lst
        if fetch:
            t = time.time() - self.start_time
            ups = messenger.get_num_updates(self.ps_ip_public, self.ps_ip_port)
            self.fetch_metric(self.UPDATES_PER_SECOND).append((t, ups))
        return self.fetch_metric(self.UPDATES_PER_SECOND)

    def get_time_loss(self, rtl=False):
        self.maintain_error()
        if rtl:
            return self.fetch_metric(self.REAL_TIME_LOSS_VS_TIME)
        else:
            return self.fetch_metric(self.LOSS_VS_TIME)

    # Fetches a metric. See BaseTask.metrics for a list of metrics
    def fetch_metric(self, key):
        return self.metrics[key]

    # This will grab the Loss v. Time by communicating with the parameter server
    def maintain_error(self):
        if self.is_dead():
            return

        out = messenger.get_last_time_error(self.ps_ip_public, self.ps_ip_port + 1)
        if out is None:
            return


        t, loss, real_time_loss, total_loss = out
        if t == 0:
            return

        if len(self.metrics[self.LOSS_VS_TIME]) == 0 or not ((t, loss) == self.metrics[self.LOSS_VS_TIME][-1]):
            self.metrics[self.LOSS_VS_TIME].append((t, loss))

            elapsed_time = time.time() - self.start_time
            current_cost = self.cost_model.get_cost(elapsed_time)
            self.metrics[self.TOTAL_LOSS_VS_TIME].append((t, total_loss / current_cost))

        self.metrics[self.REAL_TIME_LOSS_VS_TIME].append((time.time() - self.start_time, real_time_loss))
