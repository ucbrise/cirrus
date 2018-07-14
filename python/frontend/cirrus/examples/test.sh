ssh -o "StrictHostKeyChecking no" -i /home/camus/Downloads/mykey.pem ubuntu@ec2-34-212-6-172.us-west-2.compute.amazonaws.com "./parameter_server --config config.txt --nworkers 10 --rank 1 --ps_ip "172.31.5.74" --ps_port 1337 &> ps_out_1337" &
sleep 3
ssh -o "StrictHostKeyChecking no" -i /home/camus/Downloads/mykey.pem ubuntu@ec2-34-212-6-172.us-west-2.compute.amazonaws.com "./parameter_server --config config.txt --nworkers 10 --rank 2 --ps_ip "172.31.5.74" --ps_port 1337 &> error_out_1337" &
#ssh -o "StrictHostKeyChecking no" -i /home/camus/Downloads/mykey.pem ubuntu@ec2-34-212-6-172.us-west-2.compute.amazonaws.com "./parameter_server --config config.txt --nworkers 10 --rank 1 --ps_ip "172.31.5.74" --ps_port 1339 &> ps_out_1339" &
#ssh -o "StrictHostKeyChecking no" -i /home/camus/Downloads/mykey.pem ubuntu@ec2-34-212-6-172.us-west-2.compute.amazonaws.com "./parameter_server --config config.txt --nworkers 10 --rank 2 --ps_ip "172.31.5.74" --ps_port 1339 &> error_out_1339" &
