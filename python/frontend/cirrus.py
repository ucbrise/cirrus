# Logistic Regression

class LogisticRegressionTask:
    def wait(self):
        print "waiting"
        return 1,2

def dataset_handle(path, format):
    print "path: ", path, " format: ", format
    return 0


def LogisticRegression(n_workers, n_ps,
            dataset,
            access_key,
            secret_key,
            learning_rate, epsilon,
            progress_callback,
            timeout,
            threshold_loss,
            resume_model):
        print "Running Logistic Regression workload"
        return LogisticRegressionTask(
                access_key=access_key,
                secret_key=secret_key
                )

def create_random_lr_model(n):
    print "Creating random LR model with size: ", n
    return 0

# Collaborative Filtering

def CollaborativeFiltering():
    print "not implemented"


# LDA algorithm


def LDA():
    print "Not implemented"

