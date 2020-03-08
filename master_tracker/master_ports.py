import zmq
import sys
import time
import pickle

def client_connection(context, link):
    socket = context.socket(zmq.REP)
    socket.bind("tcp://{link}".format(link = link))
    #recieve request from client
    file_name, UpDown  = socket.recv_pyobj()
    if(UpDown == "0"):
        print("Upload request is recieved")
    else:
        print("Download request is recieved")
    return socket, file_name, UpDown 

def datahandler_connection(context, port):
    socket = context.socket(zmq.PUSH)
    socket.connect ("tcp://127.0.0.1:{port}".format(port = port))
    return socket
    
def upload(df2, df3, machine_check):
    table = df2.join(df3.set_index('Data Keeper ID'), on='Data Keeper ID')
    busy_ports = table[table['Busy'] == True].index
    dead_nodes = table[table['Alive'] == False].index
    table.drop(busy_ports, inplace = True)
    table.drop(dead_nodes, inplace = True)
    random_row = pd.DataFrame()
    if(not table.empty):
        random_row = table.sample()
    else:
        check = False
    return random_row
    
def download(df, df2, df3, machine_check, file_name):
    table = df2.join(df3.set_index('Data Keeper ID'), on='Data Keeper ID')
    table = table.join(df.set_index('Data Keeper ID'), on='Data Keeper ID')
    table = table[table['File Name'] == file_name]
    busy_ports = table[table['Busy'] == True].index
    dead_nodes = table[table['Alive'] == False].index
    table.drop(busy_ports, inplace = True)
    table.drop(dead_nodes, inplace = True)
    random_row = pd.DataFrame()
    if(not table.empty):
        random_row = table.sample()
    else:
        check = False
    return random_row
    
def start_client_ports(master_link, datahandler_port, ns):
    context = zmq.Context()
    client_socket, file_name, UpDown  = client_connection(context, master_link)
    datahandler_socket = datahandler_connection(context, datahandler_port)
    
    #look up tables
    df = ns.df
    df2 = ns.df2
    df3 = ns.df3 
    machine_check = True

    if(UpDown == "0"):
        random_row = upload(df2, df3, machine_check)
    else: 
        random_row = download(df, df2, df3, machine_check, file_name)
        
    #check if there is an empty machine
    if(machine_check):
        datahandler_socket.send_pyobj((random_row['Data Keeper ID'], random_row['Port']))
        #########TO BE UPDATED: IP
        client_socket.send(random_row['Port'])
        print("Data sent to datahandler and client")
    else:
        print("No empty machine :)")
    time.sleep(1)