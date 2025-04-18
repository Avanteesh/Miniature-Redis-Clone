from enum import Enum
from os import path

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
    EXIT = 'EXIT'

class Configs(Enum):
    default_port: int = 6379
    config_path: str = path.join('tmp','redis-data')  # config path
    config_file: str = 'dump.rdb'    # configuration file    
        
