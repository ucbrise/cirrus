import subprocess
import time
import sys
import os
import signal

ps_command = ["./src/parameter_server", "--nworkers=10", "--rank=1", "--config=configs/criteo_kaggle.cfg", "--ps_ip=127.0.0.1", "--ps_port=1337"]
worker_command = ["./src/parameter_server", "--nworkers=10" ,"--rank=3", "--config=configs/criteo_kaggle.cfg", "--ps_ip=127.0.0.1" ,"--ps_port=1337"]
error_command = ["./src/parameter_server" ,"--nworkers=10", "--rank=2" ,"--config=configs/criteo_kaggle.cfg", "--ps_ip=127.0.0.1", "--ps_port=1337" ,"--testing=True", "--test_iters=5", "--test_threshold=0.742"]

try:
  p1 = subprocess.Popen(ps_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  time.sleep(10)
  p2 = subprocess.Popen(worker_command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
  time.sleep(5)
  p3 = subprocess.Popen(error_command, stderr=subprocess.STDOUT)
except:
  sys.exit(-1)

while p3.poll() is None:
  time.sleep(5)
if p1.poll() is not None:
  if p1.returncode != 0:
    p2.terminate()
    print(p1.returncode)
    print("PS crashed")
    sys.exit(p1.returncode)
if p2.poll() is not None:
  if p2.returncode != 0:
    p1.terminate()
    print("worker crashed")
    sys.exit(p2.returncode)
p1.terminate();
p2.terminate();

return_code = p3.returncode
print(return_code)
sys.exit(return_code)
