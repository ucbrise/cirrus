import socket
import struct

GET_NUM_CONNS = '\x09\x00\x00\x00'
GET_NUM_UPDATES = '\x0A\x00\x00\x00'
GET_LAST_TIME_ERROR = '\x0B\x00\x00\x00'
KILL_SIGNAL = "\x0C\x00\x00\x00"


def get_num_lambdas(ip="127.0.0.1", port=1337):
    try:
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientsocket.connect((ip, port))
        clientsocket.send(GET_NUM_CONNS)
        clientsocket.settimeout(3)
        s = clientsocket.recv(32)
        return struct.unpack("I", s)[0] - 1   # Subtract 1, as we don't count the clientsocket as a connection
    except:
        return -1


def get_last_time_error(ip="127.0.0.1", port=1338):
    try:
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        clientsocket.sendto(GET_LAST_TIME_ERROR, (ip, port))
        clientsocket.settimeout(10)
        s = clientsocket.recv(192)
        return struct.unpack("ddd", s)
    except Exception, e:
        print(str(e))
        return -1, -1, -1


def get_num_updates(ip="127.0.0.1", port=1337):
    try:
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientsocket.connect((ip, port))
        clientsocket.send(GET_NUM_UPDATES)
        clientsocket.settimeout(3)
        s = clientsocket.recv(32)
        return struct.unpack("I", s)[0]
    except:
        return -1


def send_kill_signal(ip="127.0.0.1", port=1337):
    try:
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientsocket.connect((ip, port))
        clientsocket.send(KILL_SIGNAL)
        return True
    except:
        return False
