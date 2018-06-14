import cirrus

def progress_callback(loss, cost, task):
  print("Current training loss:", loss, \
        "current cost ($): ", cost)

data = ''
model = ''

lr_task = cirrus.LogisticRegression(
             n_workers = 3, n_ps = 2,
             dataset = data,
             learning_rate=0.01, epsilon=0.0001,
             progress_callback = progress_callback,
             timeout = 100,
             threshold_loss=0.48,
             resume_model = model,
             key_name='mykey',
             key_path='/home/joao/Downloads/mykey.pem',
             ps_ip='ec2-34-214-232-215.us-west-2.compute.amazonaws.com',
             ps_username='ubuntu'
             )

lr_task.run()




#model, loss = lr_task.wait()
