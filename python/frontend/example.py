import cirrus

ACCESS_KEY="..."
SECRET_KEY="..."

def progress_callback(loss, cost, task):
  print "Current training loss:", loss, \
        "current cost ($): ", cost
    #if time_elapsed > 5minutes:
    #if cost > 500$:
    #    model, loss = task.terminate()

# different dataset paths (1. S3, 2. Local file)
data = cirrus.dataset_handle(path = "s3://s3_path", \
                             format="libsvm")

# preprocess data
# using pywren (look into pywren and see if we can use to preprocess ML datasets)

# get cirrus executor
# which parameters should we have here?

#model = cirrus.create_random_lr_model(10)

'''
lr_task = cirrus.LogisticRegression(\
             n_workers = 3, n_ps = 2,
             dataset = data,
             aws_access_key=ACCESS_KEY,
             aws_secret_access_key=SECRET_KEY
             learning_rate=0.0001, epsilon=0.0001,
             progress_callback = progress_callback,
             timeout = 100,
             threshold_loss=0.48,
             resume_model = model)
'''


lr_task = cirrus.LogisticRegressionTask()
lr_task.run()

#model, loss = lr_task.wait()
