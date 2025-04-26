from enum import Enum
from os import path
from typing import Union

def listToRESPArray(arraylist: list[str]) -> str:
    result = f"*{len(arraylist)}\r\n"
    for item in arraylist:
        result += f"${len(item)}\r\n{item}\r\n"
    return result

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
    DISCARD = 'DISCARD'

class Configs(Enum):
    default_port: int = 6379
    config_path: str = path.join('tmp','redis-data')  # config path
    rdb_header: bytes = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
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
