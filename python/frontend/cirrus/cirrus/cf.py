# Collaborative Filtering

from core import BaseTask


class CollaborativeFilteringTask(BaseTask):
    def __init__(self, *args, **kwargs):
        # pass all arguments of init to parent class
        super(CollaborativeFilteringTask, self).__init__(*args, **kwargs)

    def __del__(self):
        print("Logistic Regression Task Lost")

    def define_config(self):
        if self.use_grad_threshold:
            grad_t = 1
        else:
            grad_t = 0
        config = "input_path: /home/ec2-user/cirrus/examples/ml/tests/test_mf/nf_parsed \n" + \
                 "input_type: csv \n" + \
                 "minibatch_size: %d \n" % self.minibatch_size + \
                 "s3_size: 10000 \n" + \
                 "model_type: CollaborativeFiltering \n" + \
                 "learning_rate: %f \n" % self.learning_rate + \
                 "epsilon: %lf \n" % self.epsilon + \
                 "normalize: 1 \n" + \
                 "limit_samples: 50000000 \n" + \
                 "use_bias: 1 \n" + \
                 "num_users: 480189 \n" + \
                 "num_items: 17770 \n" + \
                 "train_set: 0-5 \n" + \
                 "s3_bucket: cirrus-netflix-not-normalized"

        return config


# XXX: Is this really nessecary?
def CollaborativeFiltering(
            n_workers,
            n_ps,
            lambda_size,
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
        ps_username="ec2-user",
        opt_method="sgd",
        checkpoint_model=0,
        use_grad_threshold=False,
        grad_threshold=0.001,
        timeout=60,
        threshold_loss=0
            ):
    return CollaborativeFilteringTask(
            n_workers=n_workers,
            n_ps=n_ps,
            lambda_size=lambda_size,
            dataset=dataset,
            learning_rate=learning_rate,
            epsilon=epsilon,
            key_name=key_name,
            key_path=key_path,
            ps_ip_public=ps_ip_public,
            ps_ip_private=ps_ip_private,
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
