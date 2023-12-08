import socket
import threading
import string
IP = socket.gethostbyname(socket.gethostname())
PORT = 5566
ADDR = (IP, PORT)
PORT1=5565
ADDR1=(IP,PORT1)
SIZE = 1024
FORMAT = "utf-8"
DISCONNECT_MSG = "!!"
topic_msgs =  dict()
def printall(topic):
    return topic_msgs[topic]

def print_to_file(topic_msgs):
    #sourceFile = open('demo.txt', 'w')
    for topic in topic_msgs:
        sourceFile = open('{}.txt'.format(topic),'w')
        print(topic,"-> ",topic_msgs[topic],file=sourceFile)
    sourceFile.close()

def handle_producer(conn,addr):
    connected=True
    while connected:
        msg = conn.recv(SIZE).decode(FORMAT)
        if msg == DISCONNECT_MSG:
            connected = False
        else:
            x=msg.split(",")
            x[0].strip()
            x[1].strip()
            if x[0] not in topic_msgs.keys():
                topic_msgs[x[0]] = []
                topic_msgs[x[0]].append(x[1])
                print_to_file(topic_msgs)
            else:
                topic_msgs[x[0]].append(x[1])
                print_to_file(topic_msgs)
        print(f"[{PORT}] sent {x[1]} to TOPIC: {x[0]}")
        msg = f"Msg received: {msg}"
        conn.send(msg.encode(FORMAT)) 
        


def handle_consumer(conn,addr):
    connected=True
    while connected:
        msg = conn.recv(SIZE).decode(FORMAT)
        if msg == DISCONNECT_MSG:
            connected = False
        else:
            y=msg
            if y not in topic_msgs.keys():
                topic_msgs[y]=[]
                reply = str(printall(y))
                conn.send(reply.encode(FORMAT))
            else:
                content=printall(y)
                content=str(content)
                conn.send(content.encode(FORMAT)) 
                #printall(y)
def handle_client(conn, addr):
    print(f"[NEW CONNECTION] {ADDR} connected.")
    connected = True
    ident=conn.recv(SIZE).decode(FORMAT)
    ident=int(ident)
    if ident==1:
        print("PRODUCER CONNECTED")
        thread_producer = threading.Thread(target=handle_producer, args=(conn, addr))
        thread_producer.start()
    elif ident==2:
        print("CONSUMER CONNECTED")
        thread_consumer=threading.Thread(target=handle_consumer,args=(conn,addr))
        thread_consumer.start()
#conn.close()
def main():
    print("[STARTING] Server is starting...")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
    server.listen()
    print(f"[LISTENING] Server is listening on {IP}:{PORT}")
    client=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR1)
    flag=3
    client.send(str(flag).encode(FORMAT))
    print(f'Sent value of flag as {flag}')
    client.send(str(PORT).encode(FORMAT))
    print(f'Sent value of port as {PORT}')
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()
        print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")    
if __name__ == "__main__":
    main()