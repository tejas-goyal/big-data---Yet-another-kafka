import socket
import threading
import string
IP = socket.gethostbyname(socket.gethostname())
PORT = 5565
ADDR = (IP, PORT)
SIZE = 1024
FORMAT = "utf-8"
DISCONNECT_MSG = "!!"
set_of_ports=[]
def handle_broker(conn,port):
    print(f'in handle______broker')
    port_num=conn.recv(SIZE).decode(FORMAT)
    print(f'In handle broker with port {port_num}')
    set_of_ports.append(port_num)
    for i in set_of_ports:
        print(f'Ports connected are {i}')
    thread_hearbeat = threading.Thread(target=heartbeat,args=len(set_of_ports))

def heartbeat(portsize):
    if(portsize==1):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(100000) #Timeout in case of port not open
        try:
            s.connect((IP, 5566)) #Port ,Here 22 is port 
            print("broker1(5566) running")
        except:
            print("broker1 (5566) failed")

    if(portsize==2):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(100000) #Timeout in case of port not open
        try:
            s.connect((IP, 5567)) #Port ,Here 22 is port 
            return True
        except:
            return False

    if(portsize==3):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(100000) #Timeout in case of port not open
        try:
            s.connect((IP, 5568)) #Port ,Here 22 is port 
            return True
        except:
            return False



def handle_consumer(conn,port):
    print(f'in handle______consumer ')
    port_num =str(set_of_ports[0])
    print(f'Value of broker port is {port_num}')
    conn.send(port_num.encode(FORMAT))
    
        
def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {ADDR} connected.")
    connected = True
    ident=conn.recv(1).decode(FORMAT)
    ident=int(ident)
    print(f"Value of ident is {ident}")
    if ident==1:
        print("PRODUCER CONNECTED")
        #thread_producer = threading.Thread(target=handle_producer, args=(conn, addr))
        #thread_producer.start()
    elif ident==2:
        print("CONSUMER CONNECTED")
        thread_consumer=threading.Thread(target=handle_consumer,args=(conn,addr))
        thread_consumer.start()
    elif ident==3 or ident==3 or ident==3:
        print("Broker CONNECTED")
        thread_broker = threading.Thread(target=handle_broker,args=(conn,addr))
        thread_broker.start()
    
#conn.close()
def main():
    print("[STARTING] Zookeeper is starting...")
    zoo = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    zoo.bind(ADDR)
    zoo.listen(4)
    print(f"[LISTENING] Zookeeper is listening on {IP}:{PORT}")
    while True:
        conn, addr = zoo.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")    
if __name__ == "__main__":
    main()