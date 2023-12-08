import socket
import threading
import sys
IP = socket.gethostbyname(socket.gethostname())
PORT1 = 5565
ADDR1 = (IP,PORT1)
FORMAT = "utf-8"
zooconn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
zooconn.connect(ADDR1)
zooconn.send(str(2).encode(FORMAT))
PORT= zooconn.recv(4).decode(FORMAT)
PORT = int(PORT)
ADDR = (IP,PORT)
SIZE=1024

DISCONNECT_MSG = "!DISCONNECT"
def main():
    client=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    connected=True
    flag=2
    print(f"[CONNECTED] Consumer connected to server at {IP}:{PORT}")   
    client.send(str(flag).encode(FORMAT))
    while connected:
        flag1 = input("Type 'y' to receive messages,'n' to quit>>>")
        if flag1=='n':
            connected=False
        else:
            msg=input("Type topic name from where you want to data>>> ")
            client.send(msg.encode(FORMAT))
            broker_reply=client.recv(SIZE).decode(FORMAT)
            broker_reply=eval(broker_reply)
            print(msg , "-> ",broker_reply)

        
        #msg=client.recv(SIZE).decode(FORMAT)
        
                   
if __name__ == '__main__':
    main()
