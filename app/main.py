import socket as sock
from re import split, match
from datetime import datetime, timedelta
from threading import Thread, stack_size
from time import sleep
from os import mkdir, path
from utils import Configs
from sys import argv

class Storage:
    map: dict[str, str] = dict()
    rlist: list[str] = dict()

    
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

def addItemToList(command: list[str]) -> str:
    if len(command) < 3:
        return "(error) ERR wrong number of arguments for command LPUSH"
    second_param = command[1].split(":")  # if second parameter is a string like key:value
    if len(second_param) == 2:
        if second_param[0] not in Storage.map:
            Storage.map[second_param[0]] = {}
            Storage.map[second_param[0]]['exp'] = datetime.now() + timedelta(hours=24)
        if second_param[1] not in Storage.map[second_param[0]]:
            Storage.map[second_param[0]][second_param[1]] = [] 
        for k in range(2, len(command)):
            Storage.map[second_param[0]][second_param[1]].append(command[k])
        return f"(integer) {len(Storage.map[second_param[0]][second_param[1]])}"
    if command[1] not in Storage.rlist:
        Storage.rlist[command[1]] = list()
    for k in range(2, len(command)):
        Storage.rlist[command[1]].append(command[k])
    return f"(integer) {len(Storage.rlist[command[1]])}"

def displayList(command: list[str]) -> str:
    if len(command) < 4:
        return f"(error) ERR invalid number of arguments for \"lrange\" command"
    second_param, result = (command[1].split(":"), None)
    try:
        start, end = int(command[2]), int(command[3])
        if len(second_param) == 2 and second_param[0] in Storage.map:
            if second_param[1] in Storage.map[second_param[0]]:
                result = Storage.map[second_param[0]][second_param[1]]
        elif command[1] in Storage.rlist:
            result = Storage.rlist[command[1]]
        try:
            result = (end < 0) and result[start:(len(result)+end)+1] or result[start:end]
        except IndexError:
            return f"(error) ERR list index got out of bound"
    except ValueError:
        return f"(error) ERR invalid arguments provided must be valid integers!"    
    query_response = str()
    for k in range(len(result)-1,-1,-1):
        query_response += f"{len(result)-k}) \"{result[k]}\"\n"
    return query_response[:-1]

def popElementFromList(command: list[str], left_pop: bool=True) -> str:
    if len(command) < 2:
        return f"(error) ERR invalid number of arguments for \"lpop\" command"
    def removeElement():
        if left_pop:
            return f"{Storage.map[second_param[0]][second_param[1]].pop()}"
        first = Storage.map[second_param[0]][second_param[1]][0]
        del Storage.map[second_param[0]][second_param[1]][0]
        return first
    second_param = command[1].split(":")
    if len(second_param) == 2:
        if second_param[0] in Storage.map:
            if second_param[1] in Storage.map[second_param[0]]:
                if len(Storage.map[second_param[0]][second_param[1]]) > 1:
                    return removeElement()
            return 'nil'
        return "nil"
    if command[1] in Storage.rlist and len(Storage.rlist) >= 1:
        if left_pop == True:
            return f"{Storage.rlist[command[1]].pop()}"
        first = Storage.rlist[command[1]][0]
        del Storage.rlist[command[1]][0]
        return f"{first}"
    return 'nill'        

def showActiveKeys(command: list[str]) -> str:
    if len(command) < 2:
        return "(error) ERR one argument missing!"
    def patternFinder(pattern: str, keyelement: str):
        result = match(pattern, keyelement)
        if result == None:
            return False
        return (result.span != (0,0))
    keys = [*Storage.map.keys(), *Storage.rlist.keys()]
    pattern = command[1].replace('\'', '').replace('"', '')
    if pattern != '*':
        keys = list(filter(lambda key: patternFinder(pattern, key), keys))
    response = ""
    for k in range(len(keys)):
        response += f"{k+1}) \"{keys[k]}\"\n"
    return response[:-1]
            
def connectToClient(socks: sock.socket):
    with socks:
        while True:
            command = socks.recv(1024).decode().rstrip().lstrip()
            tokenized = split(r" \s*", command)
            response: str = None
            if tokenized[0].upper() == "PING":
                response = 'PONG\r'
            elif tokenized[0].upper() == "ECHO":
                response = (len(tokenized) < 2) and "(error) ERR no statement mentioned!" or tokenized[1]
            elif tokenized[0].upper() == "SET":
                response = setKey(tokenized)
            elif tokenized[0].upper() == "GET":
                response = getKey(tokenized)
            elif tokenized[0].upper() == "CONFIG":
                response = checkConfigurationDetails(tokenized)
            elif tokenized[0].upper() == 'LPUSH':
                response = addItemToList(tokenized)
            elif tokenized[0].upper() == 'LPOP':
                response = popElementFromList(tokenized)
            elif tokenized[0].upper() == 'LRANGE':
                response = displayList(tokenized)
            elif tokenized[0].upper() == 'RPOP':
                response = popElementFromList(tokenized, left_pop=False) # pop elements from right!
            elif tokenized[0].upper() == 'KEYS':
                response = showActiveKeys(tokenized)
            else:
                response = f"(error) ERR unknown command '{command}'"
            socks.sendall(response.encode())
    
def main():
    PORT = Configs.default_port.value  # 6379 be the default port
    if len(argv) == 3:
        if argv[1] == '--port':
            if not argv[2].isdigit():
                print("Invalid PORT!")
                return
            PORT = int(argv[2])
    print("Logs from your program will appear here!")
    sock_server = sock.create_server(("127.0.0.1", PORT))
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
