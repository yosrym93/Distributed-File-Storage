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
    return random_row,machine_check
    
def download(df, df2, df3, machine_check, file_name):
    table = df2.join(df3.set_index('Data Keeper ID'), on='Data Keeper ID')
    table = table.join(df.set_index('Data Keeper ID'), on='Data Keeper ID')
    different_files = table[table['File Name'] != file_name].index
    busy_ports = table[table['Busy'] == True].index
    dead_nodes = table[table['Alive'] == False].index
    table.drop(busy_ports, inplace = True)
    table.drop(dead_nodes, inplace = True)
    table.drop(different_files, inplace = True)
    random_row = pd.DataFrame()
    if(not table.empty):
        random_row = table.sample()
    else:
        machine_check = False
    return random_row,machine_check



def start_client_ports(client_port, datahandler_port, ns, datakeepers_ip):
    print(f"Master client ports started, listening to clients on port {client_port}, "
          f"communicating with data handler on port {datahandler_port}..")
    context = zmq.Context()
    client_socket = context.socket(zmq.REP)
    client_socket.bind("tcp://*:" + client_port)

    datahandler_socket = context.socket(zmq.PUSH)
    datahandler_socket.connect ("tcp://127.0.0.1:{port}".format(port = datahandler_port))
    while True:
        file_name, UpDown = client_connection(client_socket)
        machine_check = True

        if(UpDown == "0"):
            random_row,machine_check = upload(ns.df2, ns.df3, machine_check)
        else:
            random_row,machine_check = download(ns.df, ns.df2, ns.df3, machine_check, file_name)

        #check if there is an empty machine
        if(machine_check):
            datahandler_socket.send_pyobj((random_row['Data Keeper ID'].item(), random_row['Port'].item()))
            datakeeper_ip = datakeepers_ip[random_row['Data Keeper ID'].item()]
            datakeeper_link = datakeeper_ip + ":" + str(random_row['Port'].item())
            client_socket.send_string(datakeeper_link)
            print("Data sent to datahandler and client")
        else:
            print("No empty machine :)")
            client_socket.send_string("")
        #time.sleep(1)