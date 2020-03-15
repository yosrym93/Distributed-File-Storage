import zmq
import pandas as pd

def client_connection(socket):
    #recieve request from client
    print("befor reciving client request")
    file_name, UpDown  = socket.recv_pyobj()
    print("file name :",file_name)
    if(UpDown == "0"):
        print("Upload request is recieved")
    else:
        print("Download request is recieved")
    return file_name, UpDown
    
def upload(alive_data_keepers_table, busy_ports_table, machine_check):
    table = alive_data_keepers_table.join(busy_ports_table.set_index('Data Keeper ID'), on='Data Keeper ID', sort=True)
    table.reset_index(inplace = True)
    busy_ports = table[table['Busy'] == True].index
    dead_nodes = table[table['Alive'] == False].index
    table.drop(busy_ports, inplace = True)
    table.drop(dead_nodes, inplace = True)
    random_row = pd.DataFrame()
    if(not table.empty):
        random_row = table.sample()
    else:
        machine_check = False
    return random_row, machine_check
    
def download(files_table, alive_data_keepers_table, busy_ports_table, machine_check, file_name):
    table = alive_data_keepers_table.join(busy_ports_table.set_index('Data Keeper ID'), on='Data Keeper ID')
    table = table.join(files_table.set_index('Data Keeper ID'), on='Data Keeper ID')
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
    return random_row, machine_check


def start_client_ports(client_port, ns, datakeepers_ip,busy_check_lock):
    print(f"Master client ports started, listening to clients on port {client_port}")
    context = zmq.Context()
    client_socket = context.socket(zmq.REP)
    client_socket.bind("tcp://*:" + client_port)

    while True:
        file_name, UpDown = client_connection(client_socket)
        machine_check = True

        busy_check_lock.acquire()
        if(UpDown == "0"):
            random_row, machine_check = upload(ns.alive_data_keepers_table, ns.busy_ports_table, machine_check)
        else:
            random_row, machine_check = download(ns.files_table, ns.alive_data_keepers_table, ns.busy_ports_table, machine_check, file_name)

        #check if there is an empty machine
        if(machine_check):
            print("There is available machine at port %s",client_port)
            data_keeper_index = ns.busy_ports_table[(ns.busy_ports_table['Data Keeper ID'] == random_row['Data Keeper ID'].iloc[0]) & (ns.busy_ports_table['Port'] == random_row['Port'].iloc[0])].index
            busy_port_data_frame = ns.busy_ports_table
            busy_port_data_frame.loc[data_keeper_index, 'Busy'] = True
            ns.busy_ports_table = busy_port_data_frame
            busy_check_lock.release()
            datakeeper_ip = datakeepers_ip[int(random_row['Data Keeper ID'].iloc[0])]
            datakeeper_link = datakeeper_ip + ":" + str(random_row['Port'].iloc[0])
            client_socket.send_string(datakeeper_link)
            print("Data sent to datahandler and client")
        else:
            busy_check_lock.release()
            print("No empty machine :)")
            client_socket.send_string("")