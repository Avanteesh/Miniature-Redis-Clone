from enum import Enum
from os import path

class Configs(Enum):
    config_path: str = path.join('tmp','redis-data')  # config path
    config_file: str = 'dump.rdb'    # configuration file    
    
