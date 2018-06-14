import cirrus

def progress_callback(loss, cost, task):
  print("Current training loss:", loss, \
        "current cost ($): ", cost)

data = ''
model = ''

lr_task = cirrus.LogisticRegression(
             # number of workers and number of PSs
             n_workers = 3, n_ps = 2,
             # path to s3 bucket with input dataset
             dataset = data,
             # sgd update LR and epsilon
             learning_rate=0.01, epsilon=0.0001,
             # 
             progress_callback = progress_callback,
             # stop workload after these many seconds
             timeout = 100,
             # stop workload once we reach this loss
             threshold_loss=0.48,
             # resume execution from model stored in this s3 bucket
             resume_model = model,
             # aws key name
             key_name='mykey',
             # path to aws key
             key_path='/home/joao/Downloads/mykey.pem',
             # ip where ps lives
             ps_ip='ec2-34-214-232-215.us-west-2.compute.amazonaws.com',
             # username of VM
             ps_username='ubuntu',
             # choose between adagrad, sgd, nesterov, momentum
             opt_method = 'adagrad'
             )

lr_task.run()

#model, loss = lr_task.wait()
