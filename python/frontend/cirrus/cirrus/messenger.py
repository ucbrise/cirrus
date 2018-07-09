import struct
import socket
import time

GET_NUM_CONNS = '\x09\x00\x00\x00'
GET_LAST_TIME_ERROR = '\x0A\x00\x00\x00'


def get_num_lambdas(ip="127.0.0.1", port=1337):
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clientsocket.connect((ip, port))
    clientsocket.send(GET_NUM_CONNS)  # This sends an 8 to the param server, padded to 32 bytes
    s = clientsocket.recv(32)
    return struct.unpack("I", s)[0] - 1   # Subtract 1, as we don't count the clientsocket as a connection


# FIXME: Add some sort of timeout, in case of error out
def get_last_time_error(ip="127.0.0.1", port=1338):
    clientsocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    clientsocket.sendto(GET_LAST_TIME_ERROR, (ip, port))
    s = clientsocket.recv(128)
    return struct.unpack("dd", s)
