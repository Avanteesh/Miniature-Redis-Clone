from enum import Enum
from os import path
from typing import Union

def listToRESPArray(arraylist: list[str]) -> str:
    result = f"*{len(arraylist)}\r\n"
    for item in arraylist:
        if isinstance(item, list):
            result += listToRESPArray(item)  # is item in list is of type array convert it into RESP string recursively!
        else:
            result += f"${len(str(item))}\r\n{item}\r\n"
    return result

# command grammar
class Command(Enum):
    PING = 'PING'  # Check if the server is healthy
    ECHO = 'ECHO'  # print a statement return after it (like a print statement)
    SET = 'SET'    # set a key with value
    GET = 'GET'    # get the value of a key
    CONFIG = 'CONFIG'   # check file configuration for db
    LPUSH = 'LPUSH'    # add element into the List
    LPOP = 'LPOP'     # pop the last element from list
    LRANGE = 'LRANGE'   # display a list with specified position
    RPOP = 'RPOP'     # pop right most element from list
    KEYS = 'KEYS'     # show all the keys in the map
    INCR = 'INCR'     # increment a key (only if a value is number)
    EXIT = 'EXIT'     # stop the client from executing
    MULTI = 'MULTI'   # create a queue for batching multiple commands
    EXEC = 'EXEC'    # execute the batched commands
    DECR = 'DECR'   # Decrement a key (only if a value is number)
    XADD = 'XADD'   # add data into a Stream 
    XRANGE = 'XRANGE'  # show items inside a stream!
    TYPE = 'TYPE'   # check the type of element stored in key!
    DISCARD = 'DISCARD'  # terminate the batch 

class Configs(Enum):
    default_port: int = 6379
    config_path: str = path.join('tmp','redis-data')  # config path
    rdb_header: bytes = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==" # rdb file header
    config_file: str = 'dump.rdb'    # configuration file    
        
class Stream(object):
    """
    A Stream in Redis is a data structure that stores key value pairs with A unique ID (identifier).
    It is an Append Only data structure and all keys must be unique. Its also immutable!
    """
    def __init__(self, id: str):
        self.id = id
        self.stream_list = []

    def addItem(self, key, value):
        self.stream_list.append((key, value))      

    def flatten(self):
        result = []
        for items in self.stream_list:
            for _item in items:
                result.append(_item)
        return result 
