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
test_set: 835-836" > config_1337.txt

nohup ./parameter_server --config config_1337.txt --nworkers 2000 --rank 1 --ps_port 1337 &> ps_out_1337 & 

nohup ./parameter_server --config config_1337.txt --nworkers 20 --rank 2 --ps_ip 172.31.14.190 --ps_port 1337 &> error_out_1337 &

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
test_set: 835-836" > config_1339.txt

nohup ./parameter_server --config config_1339.txt --nworkers 3000 --rank 1 --ps_port 1339 &> ps_out_1339 & 

nohup ./parameter_server --config config_1339.txt --nworkers 30 --rank 2 --ps_ip 172.31.14.190 --ps_port 1339 &> error_out_1339 &

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
learning_rate: 0.166667 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1341.txt

nohup ./parameter_server --config config_1341.txt --nworkers 2000 --rank 1 --ps_port 1341 &> ps_out_1341 & 

nohup ./parameter_server --config config_1341.txt --nworkers 20 --rank 2 --ps_ip 172.31.14.190 --ps_port 1341 &> error_out_1341 &

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
learning_rate: 0.166667 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1343.txt

nohup ./parameter_server --config config_1343.txt --nworkers 3000 --rank 1 --ps_port 1343 &> ps_out_1343 & 

nohup ./parameter_server --config config_1343.txt --nworkers 30 --rank 2 --ps_ip 172.31.14.190 --ps_port 1343 &> error_out_1343 &

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
test_set: 835-836" > config_1345.txt

nohup ./parameter_server --config config_1345.txt --nworkers 2000 --rank 1 --ps_port 1345 &> ps_out_1345 & 

nohup ./parameter_server --config config_1345.txt --nworkers 20 --rank 2 --ps_ip 172.31.14.190 --ps_port 1345 &> error_out_1345 &

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
test_set: 835-836" > config_1347.txt

nohup ./parameter_server --config config_1347.txt --nworkers 3000 --rank 1 --ps_port 1347 &> ps_out_1347 & 

nohup ./parameter_server --config config_1347.txt --nworkers 30 --rank 2 --ps_ip 172.31.14.190 --ps_port 1347 &> error_out_1347 &

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
learning_rate: 0.071429 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1349.txt

nohup ./parameter_server --config config_1349.txt --nworkers 2000 --rank 1 --ps_port 1349 &> ps_out_1349 & 

nohup ./parameter_server --config config_1349.txt --nworkers 20 --rank 2 --ps_ip 172.31.14.190 --ps_port 1349 &> error_out_1349 &

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
learning_rate: 0.071429 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1351.txt

nohup ./parameter_server --config config_1351.txt --nworkers 3000 --rank 1 --ps_port 1351 &> ps_out_1351 & 

nohup ./parameter_server --config config_1351.txt --nworkers 30 --rank 2 --ps_ip 172.31.14.190 --ps_port 1351 &> error_out_1351 &

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
learning_rate: 0.055556 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1353.txt

nohup ./parameter_server --config config_1353.txt --nworkers 2000 --rank 1 --ps_port 1353 &> ps_out_1353 & 

nohup ./parameter_server --config config_1353.txt --nworkers 20 --rank 2 --ps_ip 172.31.14.190 --ps_port 1353 &> error_out_1353 &

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
learning_rate: 0.055556 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1355.txt

nohup ./parameter_server --config config_1355.txt --nworkers 3000 --rank 1 --ps_port 1355 &> ps_out_1355 & 

nohup ./parameter_server --config config_1355.txt --nworkers 30 --rank 2 --ps_ip 172.31.14.190 --ps_port 1355 &> error_out_1355 &

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
learning_rate: 0.045455 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1357.txt

nohup ./parameter_server --config config_1357.txt --nworkers 2000 --rank 1 --ps_port 1357 &> ps_out_1357 & 

nohup ./parameter_server --config config_1357.txt --nworkers 20 --rank 2 --ps_ip 172.31.14.190 --ps_port 1357 &> error_out_1357 &

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
learning_rate: 0.045455 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1359.txt

nohup ./parameter_server --config config_1359.txt --nworkers 3000 --rank 1 --ps_port 1359 &> ps_out_1359 & 

nohup ./parameter_server --config config_1359.txt --nworkers 30 --rank 2 --ps_ip 172.31.14.190 --ps_port 1359 &> error_out_1359 &

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
learning_rate: 0.038462 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1361.txt

nohup ./parameter_server --config config_1361.txt --nworkers 2000 --rank 1 --ps_port 1361 &> ps_out_1361 & 

nohup ./parameter_server --config config_1361.txt --nworkers 20 --rank 2 --ps_ip 172.31.14.190 --ps_port 1361 &> error_out_1361 &

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
learning_rate: 0.038462 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1363.txt

nohup ./parameter_server --config config_1363.txt --nworkers 3000 --rank 1 --ps_port 1363 &> ps_out_1363 & 

nohup ./parameter_server --config config_1363.txt --nworkers 30 --rank 2 --ps_ip 172.31.14.190 --ps_port 1363 &> error_out_1363 &

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
learning_rate: 0.033333 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1365.txt

nohup ./parameter_server --config config_1365.txt --nworkers 2000 --rank 1 --ps_port 1365 &> ps_out_1365 & 

nohup ./parameter_server --config config_1365.txt --nworkers 20 --rank 2 --ps_ip 172.31.14.190 --ps_port 1365 &> error_out_1365 &

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
learning_rate: 0.033333 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1367.txt

nohup ./parameter_server --config config_1367.txt --nworkers 3000 --rank 1 --ps_port 1367 &> ps_out_1367 & 

nohup ./parameter_server --config config_1367.txt --nworkers 30 --rank 2 --ps_ip 172.31.14.190 --ps_port 1367 &> error_out_1367 &

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
learning_rate: 0.029412 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1369.txt

nohup ./parameter_server --config config_1369.txt --nworkers 2000 --rank 1 --ps_port 1369 &> ps_out_1369 & 

nohup ./parameter_server --config config_1369.txt --nworkers 20 --rank 2 --ps_ip 172.31.14.190 --ps_port 1369 &> error_out_1369 &

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
learning_rate: 0.029412 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1371.txt

nohup ./parameter_server --config config_1371.txt --nworkers 3000 --rank 1 --ps_port 1371 &> ps_out_1371 & 

nohup ./parameter_server --config config_1371.txt --nworkers 30 --rank 2 --ps_ip 172.31.14.190 --ps_port 1371 &> error_out_1371 &

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
learning_rate: 0.026316 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1373.txt

nohup ./parameter_server --config config_1373.txt --nworkers 2000 --rank 1 --ps_port 1373 &> ps_out_1373 & 

nohup ./parameter_server --config config_1373.txt --nworkers 20 --rank 2 --ps_ip 172.31.14.190 --ps_port 1373 &> error_out_1373 &

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
learning_rate: 0.026316 
epsilon: 0.000100 
model_bits: 19 
s3_bucket: cirrus-criteo-kaggle-19b-random 
use_grad_threshold: 0 
grad_threshold: 0.001000 
train_set: 0-824 
test_set: 835-836" > config_1375.txt

nohup ./parameter_server --config config_1375.txt --nworkers 3000 --rank 1 --ps_port 1375 &> ps_out_1375 & 

nohup ./parameter_server --config config_1375.txt --nworkers 30 --rank 2 --ps_ip 172.31.14.190 --ps_port 1375 &> error_out_1375 &

