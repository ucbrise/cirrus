import time
import threading

payload = '{"num_task": %d, "num_workers": %d, "ps_ip": \"%s\", "ps_port": %d}' \
            % (10, 10, "", 100)

start_time = time.time()

for i in range(100):
    try:
        response = client.invoke(
            FunctionName="myfunc",
            InvocationType='Event',
            LogType='Tail',
            Payload=payload)
    except:
        print "client.invoke exception caught"

print(time.time() - start_time)



def func():
    try:
        response = client.invoke(
            FunctionName="myfunc",
            InvocationType='Event',
            LogType='Tail',
            Payload=payload)
    except:
        print "client.invoke exception caught"

start_time = time.time()
for i in range(100):

    t = threading.Thread(target=func)
    t.start()

print(time.time() - start_time)
