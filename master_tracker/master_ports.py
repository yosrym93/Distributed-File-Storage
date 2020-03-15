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
    
def upload(df2, df3, machine_check):
    table = df2.join(df3.set_index('Data Keeper ID'), on='Data Keeper ID', sort=True)
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
    return random_row, machine_check



def start_client_ports(client_port, datahandler_port, ns, datakeepers_ip,busy_check_lock):
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

        busy_check_lock.acquire()
        if(UpDown == "0"):
            random_row, machine_check = upload(ns.df2, ns.df3, machine_check)
        else:
            random_row, machine_check = download(ns.df, ns.df2, ns.df3, machine_check, file_name)

        #check if there is an empty machine
        if(machine_check):
            #datahandler_socket.send_pyobj((random_row['Data Keeper ID'].item(), random_row['Port'].item()))
            print("There is available machine at port %s",client_port)
            data_keeper_index = ns.df3[(ns.df3['Data Keeper ID'] == random_row['Data Keeper ID'].iloc[0]) & (ns.df3['Port'] == random_row['Port'].iloc[0])].index
            busy_port_data_frame = ns.df3
            busy_port_data_frame.loc[data_keeper_index, 'Busy'] = True
            ns.df3 = busy_port_data_frame
            busy_check_lock.release()
            datakeeper_ip = datakeepers_ip[int(random_row['Data Keeper ID'].iloc[0])]
            datakeeper_link = datakeeper_ip + ":" + str(random_row['Port'].iloc[0])
            client_socket.send_string(datakeeper_link)
            print("Data sent to datahandler and client")
        else:
            busy_check_lock.release()
            print("No empty machine :)")
            client_socket.send_string("")