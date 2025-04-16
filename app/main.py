import socket as sock
from re import split
from datetime import datetime, timedelta
from threading import Thread, stack_size
from time import sleep
from os import mkdir, path
from enum import Enum

class Configs(Enum):
    config_path: str = path.join('tmp','redis-data')  # config path
    config_file: str = 'dump.rdb'    # configuration file    

class Storage:
    map: dict[str, str] = {}
    
def setKey(command: list[str]) -> str:
    # set value to the hashmap
    # default TTL is 86400
    if len(command) < 3:
        return "(error) ERR key and value must be specified!"
    DEFAULT_TTL = (datetime.now() + timedelta(seconds=86400))
    Storage.map[command[1]] = {'value': command[2], 'exp': DEFAULT_TTL}  # default expiry time
    if len(command) == 5:
        STATIC_TTL = datetime.now()
        match command[3].upper():
            case 'PX':
                STATIC_TTL += timedelta(milliseconds=int(command[4]))
                Storage.map[command[1]] = {
                  'value': command[2], 'exp': STATIC_TTL
                }
            case 'EX':
                STATIC_TTL += timedelta(seconds=int(command[4]))
                Storage.map[command[1]] = {
                  'value': command[2], 'exp': STATIC_TTL
                }
            case _:
                return '(error) ERR invalid argument for TTL field!'
    return "ok"

def checkConfigurationDetails(command):
    def init_configs():
        if not path.exists('tmp'):
            mkdir('tmp')
        if not path.exists(Configs.config_path.value):
            mkdir(Configs.config_path.value)
    if len(command) < 3:
        return f'(error) ERR no arguments have been provided!'
    elif command[1].upper() == 'GET':
        if command[2].upper() == 'DIR':
            init_configs()
            return f"1) \"dir\"\n2) \"{Configs.config_path.value}\""
        if command[2].upper() == 'DBFILENAME':
            init_configs()
            if not path.exists(path.join(Configs.config_path.value, Configs.config_file.value)):
                with open(path.join(Configs.config_path.value, Configs.config_file.value), 'wb') as _:
                    pass
            return f"1) \"dbfilename\"\n2) \"{Configs.config_file.value}\""

def checkForExpiryKeys() -> None:
    # check if any of the keys have reached their expiry date!
    while True:
        init_dict = Storage.map.copy()
        target_keys = []
        for key, values in init_dict.items():
            if values['exp'] <= datetime.now():
                if key in init_dict:
                    target_keys.append(key)
        for keys in target_keys:
            del init_dict[keys]    
        if len(target_keys) != 0:
            Storage.map = init_dict
        
def getKey(command: list[str]) -> str:
    # get value from the hashmap
    if len(command) < 2:
        return "(error) ERR key name not provided!"
    elif command[1] in Storage.map:
        return Storage.map[command[1]]['value']
    return '(nil)'
    
def connectToClient(socks: sock.socket):
    with socks:
        while True:
            command = socks.recv(1024).decode().rstrip().lstrip()
            tokenized = split(r" \s*", command)
            response: str = None
            if tokenized[0].upper() == "PING":
                response = 'PONG\r'
            elif tokenized[0].upper() == "ECHO":
                if len(tokenized) < 2:
                    response = "(error) ERR no statement mentioned!"
                else:
                    response = tokenized[1]
            elif tokenized[0].upper() == "SET":
                response = setKey(tokenized)
            elif tokenized[0].upper() == "GET":
                response = getKey(tokenized)
            elif tokenized[0].upper() == "CONFIG":
                response = checkConfigurationDetails(tokenized)
            else:
                response = f"(error) ERR unknown command '{command}'"
            socks.sendall(response.encode())
    
def main():
    print("Logs from your program will appear here!")
    sock_server = sock.create_server(("127.0.0.1", 6379))
    while True:
        connection, address = sock_server.accept()
        con_thread: Thread = Thread(target=connectToClient, args=[connection]) # connection thread!
        data_ttl: Thread = Thread(target=checkForExpiryKeys, args=[])  # check for any expired keys!
        data_ttl.start()
        con_thread.start()
        data_ttl.join()
        con_thread.join()
        
if __name__ == "__main__":
    main()
