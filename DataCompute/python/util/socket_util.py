# encoding: utf-8
"""
套接字工具类
@author Yuriseus
@create 2016-8-24 10:27
"""
import socket
import struct


class SocketUtil(object):

    @staticmethod
    def ip2int(ip):
        packed_ip = socket.inet_aton(ip)
        return int(struct.unpack("!L", packed_ip)[0])

    @staticmethod
    def int2ip(int_ip):
        return socket.inet_ntoa(struct.pack('!L', int_ip))
