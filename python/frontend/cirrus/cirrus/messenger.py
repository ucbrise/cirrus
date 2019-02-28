import socket
import struct

GET_NUM_CONNS = '\x09\x00\x00\x00'
GET_NUM_UPDATES = '\x0A\x00\x00\x00'
GET_LAST_TIME_ERROR = '\x0B\x00\x00\x00'
KILL_SIGNAL = "\x0C\x00\x00\x00"


# TODO: There's something in the exceptions that hogs a connection spot on PS

def get_num_lambdas(ps):
    try:
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientsocket.connect((ps.public_ip(), ps.ps_port()))
        clientsocket.send(GET_NUM_CONNS)
        clientsocket.settimeout(3)
        s = clientsocket.recv(32)
        return struct.unpack("I", s)[0] - 1   # Subtract 1, as we don't count the clientsocket as a connection
    except Exception, e:
        clientsocket.close()
        return None


def get_last_time_error(ps):
    try:
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        clientsocket.sendto(GET_LAST_TIME_ERROR, (ps.public_ip(), ps.error_port()))
        clientsocket.settimeout(10)
        s = clientsocket.recv(256)      # Receives a packet of 3 floats or 256 bytes
        res = struct.unpack("ddd", s) # Unpack 3 floats
        return (res[0], res[1], res[2], res[2])
    except Exception, e:
        return None


def get_num_updates(ps):
    try:
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientsocket.connect((ps.public_ip(), ps.ps_port()))
        clientsocket.send(GET_NUM_UPDATES)
        clientsocket.settimeout(3)
        s = clientsocket.recv(32)
        return struct.unpack("I", s)[0]
    except Exception, e:
        clientsocket.close()
        return None


def send_kill_signal(ip="127.0.0.1", port=1337):
    try:
        clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientsocket.connect((ip, port))
        clientsocket.send(KILL_SIGNAL)
        return True
    except Exception, e:
        clientsocket.close()
        return False

if __name__ == "__main__":

    print get_last_time_error("18.237.161.88", 1338)
