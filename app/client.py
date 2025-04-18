from sys import argv
import socket

def main():
    HOST, PORT = ('127.0.0.1', 6379)
    if len(argv) == 3:
        HOST, PORT = argv[1], argv[2]
    with socket.create_connection((HOST, PORT)) as conn:
        while True:
            command = input(f"{HOST}:{PORT}> ").lstrip().rstrip()
            conn.sendall(command.encode())
            recieved = conn.recv(1024)
            if recieved == "closed":
                print(recieved.decode("utf-8")) 
                conn.close()
                return
            print(recieved.decode("utf-8"))

if __name__ == "__main__":
    main()



