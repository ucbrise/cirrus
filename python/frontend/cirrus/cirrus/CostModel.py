import threading
import paramiko
import time
import os
import boto3
from threading import Thread

class CostModel:
    def __init__(self,
            vm_type,
            num_vms,
            s3_space_mb,
            num_workers,
            lambda_size):

        self.vm_type = vm_type
        self.num_vms = num_vms
        self.s3_space_mb = s3_space_mb
        self.num_workers = num_workers
        self.lambda_size = lambda_size
        
    def get_cost_per_second(self):
        # cost of smallest lambda (128MB) per hour
        lambda_cost_base_h = 0.007488
        total_lambda_cost_h = (self.lambda_size / 128.0 * lambda_cost_base_h) \
                * self.num_workers
        total_lambda_cost = total_lambda_cost_h / (60 * 60)

        # vm_cost
        vm_to_cost = {
            'm5.large' : 0.096 # demand price per hour
        }

        if self.vm_type not in vm_to_cost:
            raise "Unknown VM type"

        total_vm_cost_h = vm_to_cost[self.vm_type] * self.num_vms
        total_vm_cost = total_vm_cost_h / (60 * 60)


        # S3 cost
        # s3 costs $0.023 per GB per month
        s3_cost_gb_h = 0.023 / (30 * 24)
        total_s3_cost_h = s3_cost_gb_h * (1.0 * self.s3_space_mb / 1024)
        total_s3_cost = total_s3_cost_h / (60 * 60)

        return total_lambda_cost + total_vm_cost + total_s3_cost


    # compute cos ($) of running worload
    # with given number of vms of specific type,
    # with lambdas of specific size
    # and s3 storage of specific size
    def get_cost(self, num_secs):
        # cost of smallest lambda (128MB) per hour
        lambda_cost_base_h = 0.007488
        total_lambda_cost_h = (self.lambda_size / 128.0 * lambda_cost_base_h) \
                * self.num_workers
        total_lambda_cost = total_lambda_cost_h / (60 * 60) * num_secs

        # vm_cost
        vm_to_cost = {
            'm5.large' : 0.096 # demand price per hour
        }

        if self.vm_type not in vm_to_cost:
            raise "Unknown VM type"

        total_vm_cost_h = vm_to_cost[self.vm_type] * self.num_vms
        total_vm_cost = total_vm_cost_h / (60 * 60) * num_secs


        # S3 cost
        # s3 costs $0.023 per GB per month
        s3_cost_gb_h = 0.023 / (30 * 24)
        total_s3_cost_h = s3_cost_gb_h * (1.0 * self.s3_space_mb / 1024)
        total_s3_cost = total_s3_cost_h / (60 * 60) * num_secs

        return total_lambda_cost + total_vm_cost + total_s3_cost
