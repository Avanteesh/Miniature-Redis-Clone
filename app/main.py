import socket as sock
from re import split
from datetime import datetime, timedelta
from threading import Thread, stack_size
from time import sleep


class Storage:
    map: dict[str, str] = {}
    
def setKey(command: list[str]) -> str:
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
    if len(command) < 2:
        return "(error) ERR key name not provided!"
    elif command[1] in Storage.map:
        return Storage.map[command[1]]['value']
    return '(nil)'
    
def connectToClient(socks: sock.socket):
    with socks:
        while True:
            command = socks.recv(1024).decode().rstrip().lstrip()
            lexical_token = split(r" \s*", command)
            response: str = None
            if lexical_token[0].upper() == "PING":
                response = '+PONG\r\n'
            elif lexical_token[0].upper() == "ECHO":
                response = lexical_token[1]
            elif lexical_token[0].upper() == "SET":
                response = setKey(lexical_token)
            elif lexical_token[0].upper() == "GET":
                response = getKey(lexical_token)
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
