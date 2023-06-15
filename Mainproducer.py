import socket
import threading
import sys

IP = socket.gethostbyname(socket.gethostname())
PORT=5566
PORT1=5565
ADDRZ=(IP,PORT1)
ADDR = (IP,PORT)
ADDR1 = (IP,PORT+1)
ADDR2 = (IP,PORT+2)
SIZE=1024
FORMAT = "utf-8"
DISCONNECT_MSG = "!DISCONNECT"

def main():
    client=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    client1=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client1.connect(ADDR1)
    client2=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client2.connect(ADDR2)
    #client3=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    #client3.connect(ADDRZ)
    flag=1
    client.send(str(flag).encode(FORMAT))
    print(f"[CONNECTED] Producer connected to server at {ADDR}") 
    client1.send(str(flag).encode(FORMAT))
    print(f"[CONNECTED] Producer connected to server at {ADDR1}") 
    client2.send(str(flag).encode(FORMAT))
    print(f"[CONNECTED] Producer connected to server at {ADDR2}") 
    #client3.send(str(flag).encode(FORMAT))
    #print(f"[CONNECTED] Producer connected to Zookeeper at {ADDRZ}")   
    connected=True
    while connected:
        flag1 = input("Type 'y' to send data , 'n' to quit>>>")
        if flag1=='n':
            connected=False
        else:
            msg=input("Enter TOPIC NAME and the MESSAGE >>> ")
            client.send(msg.encode(FORMAT))
            client1.send(msg.encode(FORMAT))
            client2.send(msg.encode(FORMAT))
        msg=client.recv(SIZE).decode(FORMAT)
        msg=client1.recv(SIZE).decode(FORMAT)
        msg=client2.recv(SIZE).decode(FORMAT)
        
                   
if __name__ == '__main__':
    main() 
