import struct
import socket
import time


def get_num_lambdas(ip, port):
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect(('localhost', 1337))
    clientsocket.send('\x08\x00\x00\x00')
    s = clientsocket.recv(32)
    return struct.unpack("HH", s)[0]

def get_time_error(ip, port):
    pass;





if __name__ == "__main__":
    get_num_lambdas((     

