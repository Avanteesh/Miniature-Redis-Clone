from enum import Enum
from os import path
from typing import Union

# command grammar
class Command(Enum):
    PING = 'PING'
    ECHO = 'ECHO'
    SET = 'SET'
    GET = 'GET'
    CONFIG = 'CONFIG'
    LPUSH = 'LPUSH'
    LPOP = 'LPOP'
    LRANGE = 'LRANGE'
    RPOP = 'RPOP'
    KEYS = 'KEYS'
    INCR = 'INCR'
    EXIT = 'EXIT'
    MULTI = 'MULTI'
    EXEC = 'EXEC'
    XADD = 'XADD'  
    TYPE = 'TYPE'

class Configs(Enum):
    default_port: int = 6379
    config_path: str = path.join('tmp','redis-data')  # config path
    config_file: str = 'dump.rdb'    # configuration file    
        
class Stream(object):
    """
    A Stream in Redis is a data structure that stores key value pairs with A unique ID (identifier).
    It is an Append Only data structure and all keys must be unique
    """
    def __init__(self, id: str):
        self.id = id
        self.__stream_list = []

    def addItem(self, key, value):
        self.__stream_list.append((key, value))        

    def __repr__(self):
        response = f"{self.id}\n"
        for k in range(len(self.__stream_list)):
            response += f"{k+1}) {self.__stream_list[k][0]}\n{k+2}) {self.__stream_list[k][1]}\n"
        return response
