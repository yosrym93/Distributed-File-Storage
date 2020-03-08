import zmq
import sys
import time
import pickle
import pandas as pd

def client_connection(socket):
    #recieve request from client
    file_name, UpDown  = socket.recv_pyobj()
    if(UpDown == "0"):
        print("Upload request is recieved")
    else:
        print("Download request is recieved")
    return file_name, UpDown
    
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
        machine_check = False
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
        machine_check = False
    return random_row


def start_client_ports(client_port, datahandler_port, ns, datakeepers_ip):
    print(f"Master client ports started, listening to clients on port {client_port}, "
          f"communicating with data handler on port {datahandler_port}..")
    context = zmq.Context()
    client_socket  = context.socket(zmq.REP)
    client_socket.bind("tcp://*:" + client_port)

    datahandler_socket = context.socket(zmq.PUSH)
    datahandler_socket.connect ("tcp://127.0.0.1:{port}".format(port = datahandler_port))
    while True:
        file_name, UpDown = client_connection(client_socket)
        machine_check = True

        if(UpDown == "0"):
            random_row = upload(ns.df2, ns.df3, machine_check)
        else:
            random_row = download(ns.df, ns.df2, ns.df3, machine_check, file_name)

        #check if there is an empty machine
        if(machine_check):
            datahandler_socket.send_pyobj((random_row['Data Keeper ID'], random_row['Port']))
            datakeeper_ip = datakeepers_ip[random_row['Data Keeper ID']]
            datakeeper_link = datakeeper_ip + ":" + random_row['Port']
            client_socket.send(datakeeper_link)
            print("Data sent to datahandler and client")
        else:
            print("No empty machine :)")
        time.sleep(1)