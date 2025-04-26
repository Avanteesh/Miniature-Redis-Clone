import socket as sock
from re import split, match
from datetime import datetime, timedelta
from threading import Thread, stack_size
from time import sleep
from os import mkdir, path
from utils import Configs, Command, Stream, listToRESPArray
from sys import argv
from time import time
from base64 import b64decode

class Storage:
    map: dict[str, str] = dict()
    rlist: dict[str, list[str]] = dict()
    streams: dict[str, dict[str, list[Stream]]] = dict()
    
def setKey(command: list[str]) -> str:
    # set value to the hashmap
    # default TTL is 86400
    if len(command) < 3:
        return "+(error) ERR key and value must be specified!\r\n"
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
                return '+(error) ERR invalid argument for TTL field!\r\n'
    return "+OK\r\n"

def checkConfigurationDetails(command) -> str:
    def init_configs():
        if not path.exists('tmp'):
            mkdir('tmp')
        if not path.exists(Configs.config_path.value):
            mkdir(Configs.config_path.value)
    if len(command) < 3:
        return f'+(error) ERR no arguments have been provided!\r\n'
    elif command[1].upper() == Command.GET.value:
        if command[2].upper() == 'DIR':
            init_configs()
            return listToRESPArray([Configs.config_path.value])
        if command[2].upper() == 'DBFILENAME':
            init_configs()
            if not path.exists(path.join(Configs.config_path.value, Configs.config_file.value)):
                with open(path.join(Configs.config_path.value, Configs.config_file.value), 'wb') as f1:
                    f1.write(b64decode(Configs.rdb_header.value))
            return listToRESPArray(['dbfilename', Configs.config_file.value])

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
        return "+(error) ERR key name not provided!\r\n"
    elif command[1] in Storage.map:
        return f"+{Storage.map[command[1]]['value']}\r\n"
    return '+(nil)\r\n'

def addItemToList(command: list[str]) -> str:
    if len(command) < 3:
        return "+(error) ERR wrong number of arguments for command LPUSH\r\n"
    second_param = command[1].split(":")  # if second parameter is a string like key:value
    if len(second_param) == 2:
        if second_param[0] not in Storage.map:
            Storage.map[second_param[0]] = {}
            Storage.map[second_param[0]]['exp'] = datetime.now() + timedelta(hours=24)
        if second_param[1] not in Storage.map[second_param[0]]:
            Storage.map[second_param[0]][second_param[1]] = [] 
        for k in range(2, len(command)):
            Storage.map[second_param[0]][second_param[1]].append(command[k])
        return f"+(integer) {len(Storage.map[second_param[0]][second_param[1]])}\r\n"
    if command[1] not in Storage.rlist:
        Storage.rlist[command[1]] = list()
    for k in range(2, len(command)):
        Storage.rlist[command[1]].append(command[k])
    return f"+(integer) {len(Storage.rlist[command[1]])}\r\n"

def displayList(command: list[str]) -> str:
    if len(command) < 4:
        return f"+(error) ERR invalid number of arguments for \"lrange\" command\r\n"
    second_param, result = (command[1].split(":"), None)
    try:
        start, end = int(command[2]), int(command[3])
        if len(second_param) == 2 and second_param[0] in Storage.map:
            if second_param[1] in Storage.map[second_param[0]]:
                result = Storage.map[second_param[0]][second_param[1]]
        elif command[1] in Storage.rlist:
            result = Storage.rlist[command[1]]
        if result is None:
            return "+(empty array)\r\n"
        try:
            result = (end < 0) and result[start:(len(result)+end)+1] or result[start:end]
        except IndexError:
            return f"+(error) ERR list index got out of bound\r\n"
    except ValueError:
        return f"+(error) ERR invalid arguments provided must be valid integers!\r\n"    
    return listToRESPArray(result)

def popElementFromList(command: list[str], left_pop: bool=True) -> str:
    if len(command) < 2:
        return f"+(error) ERR invalid number of arguments for \"lpop\" command\r\n"
    def removeElement():
        if left_pop:
            return f"+{Storage.map[second_param[0]][second_param[1]].pop()}\r\n"
        first = Storage.map[second_param[0]][second_param[1]][0]
        del Storage.map[second_param[0]][second_param[1]][0]
        return f"+{first}\r\n"
    second_param = command[1].split(":")
    if len(second_param) == 2:
        if second_param[0] in Storage.map:
            if second_param[1] in Storage.map[second_param[0]]:
                if len(Storage.map[second_param[0]][second_param[1]]) > 1:
                    return removeElement()
            return '+(nil)\r\n'
        return "+(nil)\r\n"
    if command[1] in Storage.rlist and len(Storage.rlist) >= 1:
        if left_pop == True:
            return f"+{Storage.rlist[command[1]].pop()}\r\n"
        first = Storage.rlist[command[1]][0]
        del Storage.rlist[command[1]][0]
        return f"+{first}\r\n"
    return '+(nil)\r\n'        

def showActiveKeys(command: list[str]) -> str:
    if len(command) < 2:
        return "+(error) ERR one argument missing!\r\n"
    def patternFinder(pattern: str, keyelement: str):
        result = match(pattern, keyelement)
        if result == None:
            return False
        return (result.span != (0,0))
    keys = [*Storage.map.keys(), *Storage.rlist.keys()]
    pattern = command[1].replace('\'', '').replace('"', '')
    if pattern != '*':
        keys = list(filter(lambda key: patternFinder(pattern, key), keys))
    response = "+"
    for k in range(len(keys)):
        response += f"{k+1}) \"{keys[k]}\"\n"
    return response[:-1] + "\r\n" 

def incrementKey(command: list[str]) -> str:
    if len(command) != 2:
        return "+(error) ERR two arguments required!\r\n"
    elif command[1] not in Storage.map:
        return "+(error) nil values can't be incremented!\r\n"
    try:
        key_val = int(Storage.map[command[1]]['value'])
        Storage.map[command[1]]['value'] = str(key_val + 1)
    except ValueError:
        return f"+(error) the key \"{command[1]}\" is not valid number\r\n"
    return "+OK\r\n"

def appendStreamLog(command: list[str]) -> str:
    if len(command) < 5:
        return "+(error) ERR invalid number of arguments. You need to specify a keyname, Unique Key and keyvalue pairs\r\n"
    def helper(unique_key: str):
        top_key, bottom_key = unique_key.split("-")
        if top_key not in Storage.streams[command[1]]:
            Storage.streams[command[1]] = []    # list of streams
        stream: Stream = Stream(int(bottom_key)) 
        if len(command[3:]) % 2 != 0:
            return "+(error) values of all keys must be specified\r\n"
        u = 3
        while u < len(command)-1:
            stream.addItem(command[u], command[u+1])
            u += 1
        if len(Storage.streams[command[1]]) >= 1:
            existing_top = Storage.streams[command[1]][toplevel][-1].id # the top most key on the second key
            # comparing the prior key with the new key!
            if existing_top >= int(bottom_key):
                return "+(error) ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
        Storage.streams[command[1]][top_key].append(stream)    
        return f"+\"{unique_key}\"\r\n"
    if command[1] not in Storage.streams:
        Storage.streams[command[1]] = dict()
    unique_key = command[2]
    if unique_key == "0-0":
        return "+(error) ERR The ID specified in XADD must be greater than 0-0\r\n"
    elif unique_key == "*":
        unix_timestamp = f"{time()*1e7}-0"
        return helper(unix_timestamp)
    elif match(r"\d+-\d+", unique_key) is not None:    
        return helper(unique_key)
    elif match(r"\d+", unique_key) is not None:
        return helper(f"{unique_key}-0")

def displayStreamResponse(command: list[str]) -> str:
    if len(command) < 4:
        return "+(error) ERR stream keys starting and ending range not specified!\r\n"
    if command[1] not in Storage.streams:
        return "+(empty array)\r\n"

def getKeyType(command: list[str]) -> str:
    if len(command) != 2:
        return "+(error) Two arguments required for type\r\n"
    if command[1] in Storage.map:
        return '+string\r\n'
    elif match(r"\w+:\w+", command[1]) is not None:
        keys = command[1].split(":")
        if keys[0] in Storage.map:
            if keys[1] in Storage.map[keys[0]]:
                if Storage.map[keys[0]][keys[1]]:
                    return '+list\r\n'
        return '+none\r\n'
    elif command[1] in Storage.streams:
        return '+stream\r\n'

def parseRespString(respstr: str) -> list[str]:
    tokens = respstr.split("\r\n")
    result = []
    for k in range(2, len(tokens), 2):
        result.append(tokens[k])
    return result
    
def connectToClient(socks: sock.socket):
    with socks:
        queueing_mode, batch_queue = False, []
        while True:
            command = parseRespString(socks.recv(1024).decode().rstrip().lstrip())
            response: str = str()
            if len(command) != 0:
                match command[0].upper():
                    case Command.PING.value:
                        response = '+PONG\r\n'
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.ECHO.value:
                        response = (len(command) < 2) and "+(error) ERR no statement mentioned!\r\n" or f"+{command[1]}\r\n"
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.SET.value:
                        response = setKey(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.GET.value:
                        response = getKey(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.CONFIG.value:
                        response = checkConfigurationDetails(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.LPUSH.value:
                        response = addItemToList(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.LPOP.value:
                        response = popElementFromList(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.LRANGE.value:
                        response = displayList(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.RPOP.value:
                        response = popElementFromList(command, left_pop=False) # pop elements from right!
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.KEYS.value:
                        response = showActiveKeys(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.INCR.value:
                        response = incrementKey(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = "+QUEUED\r\n"
                    case Command.MULTI.value:
                        if queueing_mode:
                            response = "+(error) ERR already in queue\r\n"    
                        queueing_mode = True
                        response = '+OK\r\n'
                    case Command.EXEC.value:
                        if not queueing_mode:
                            response = "+(error) Commands were never queued, You need to use the MULTI command\r\n"
                        else:
                            response = f"*{len(batch_queue)}\r\n"  # resp bulkstring for array representation
                            for item in batch_queue:
                                response += f"{item}"
                            queueing_mode = False
                            batch_queue.clear()
                    case Command.XADD.value:
                        response = appendStreamLog(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = "+QUEUED\r\n"
                    case Command.TYPE.value:
                        response = getKeyType(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = '+QUEUED\r\n'
                    case Command.DISCARD.value:
                        if not queueing_mode:
                            response = "+(error) ERR DISCARD without MULTI\r\n"
                        else:
                            queueing_mode = False
                            batch_queue.clear()
                            response = "+OK\r\n"
                    case Command.XRANGE.value:
                        response = displayStreamResponse(command)
                        if queueing_mode:
                            batch_queue.append(response)
                            response = "+QUEUED\r\n"
                    case Command.EXIT.value:
                        socks.sendall(b"+closed\r\n")
                        socks.close()
                        return
                    case _:
                        response = f"+(error) ERR unknown command '{command[0]}'\r\n"
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
