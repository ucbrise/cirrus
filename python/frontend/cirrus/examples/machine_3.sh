echo "input_path: /mnt/efs/criteo_kaggle/train.csv 
input_type: csv
num_classes: 2 
num_features: 13 
limit_cols: 14 
normalize: 1 
limit_samples: 50000000 
s3_size: 50000 
use_bias: 1 
model_type: LogisticRegression 
minibatch_size: 20 
learning_rate: 0.100000 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-840" > config_1341.txt

nohup ./parameter_server --config config_1341.txt --nworkers 40 --rank 1 --ps_port 1341 &> ps_out_1341 & 

nohup ./parameter_server --config config_1341.txt --nworkers 20 --rank 2 --ps_ip 172.31.6.212 --ps_port 1341 &> error_out_1341 &

echo "input_path: /mnt/efs/criteo_kaggle/train.csv 
input_type: csv
num_classes: 2 
num_features: 13 
limit_cols: 14 
normalize: 1 
limit_samples: 50000000 
s3_size: 50000 
use_bias: 1 
model_type: LogisticRegression 
minibatch_size: 20 
learning_rate: 0.300000 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-840" > config_1349.txt

nohup ./parameter_server --config config_1349.txt --nworkers 40 --rank 1 --ps_port 1349 &> ps_out_1349 & 

nohup ./parameter_server --config config_1349.txt --nworkers 20 --rank 2 --ps_ip 172.31.6.212 --ps_port 1349 &> error_out_1349 &

echo "input_path: /mnt/efs/criteo_kaggle/train.csv 
input_type: csv
num_classes: 2 
num_features: 13 
limit_cols: 14 
normalize: 1 
limit_samples: 50000000 
s3_size: 50000 
use_bias: 1 
model_type: LogisticRegression 
minibatch_size: 20 
learning_rate: 0.400000 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-840" > config_1357.txt

nohup ./parameter_server --config config_1357.txt --nworkers 40 --rank 1 --ps_port 1357 &> ps_out_1357 & 

nohup ./parameter_server --config config_1357.txt --nworkers 20 --rank 2 --ps_ip 172.31.6.212 --ps_port 1357 &> error_out_1357 &

echo "input_path: /mnt/efs/criteo_kaggle/train.csv 
input_type: csv
num_classes: 2 
num_features: 13 
limit_cols: 14 
normalize: 1 
limit_samples: 50000000 
s3_size: 50000 
use_bias: 1 
model_type: LogisticRegression 
minibatch_size: 20 
learning_rate: 0.500000 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-840" > config_1365.txt

nohup ./parameter_server --config config_1365.txt --nworkers 40 --rank 1 --ps_port 1365 &> ps_out_1365 & 

nohup ./parameter_server --config config_1365.txt --nworkers 20 --rank 2 --ps_ip 172.31.6.212 --ps_port 1365 &> error_out_1365 &

