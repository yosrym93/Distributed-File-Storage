import zmq
import pandas as pd


upload = '0'
download = '1'


def client_connection(socket):
    file_name, transfer_mode = socket.recv_pyobj()
    if transfer_mode == upload:
        print("Upload request received, file name: {}".format(file_name))
    else:
        print("Download request received, file name: {}".format(file_name))
    return file_name, transfer_mode


def upload_file(alive_data_keepers_table, busy_ports_table, machine_check):
    table = alive_data_keepers_table.join(busy_ports_table.set_index('Data Keeper ID'), on='Data Keeper ID', sort=True)
    table.reset_index(inplace=True)
    dropped_indices = table[(table['Busy'] == True) | (table['Alive'] == False)].index
    table.drop(dropped_indices, inplace=True)
    random_row = pd.DataFrame()
    if not table.empty:
        random_row = table.sample()
    else:
        machine_check = False
    return random_row, machine_check


def download_file(files_table, alive_data_keepers_table, busy_ports_table, machine_check, file_name):
    table = alive_data_keepers_table.join(busy_ports_table.set_index('Data Keeper ID'), on='Data Keeper ID')
    table = table.join(files_table.set_index('Data Keeper ID'), on='Data Keeper ID')
    table = table[table['File Name'] == file_name]
    dropped_indices = table[(table['Busy'] == True) | (table['Alive'] == False) |
                            (table['Is Replicating'] == True)].index
    table.drop(dropped_indices, inplace=True)
    random_row = pd.DataFrame()
    if not table.empty:
        random_row = table.sample()
    else:
        machine_check = False
    return random_row, machine_check


def start_client_ports(client_port, ns, data_keepers_ip, busy_check_lock):
    print(f"Master client ports started, listening to clients on port {client_port}")
    context = zmq.Context()
    client_socket = context.socket(zmq.REP)
    client_socket.bind("tcp://*:" + client_port)

    while True:
        file_name, transfer_mode = client_connection(client_socket)
        machine_check = True

        busy_check_lock.acquire()
        if transfer_mode == upload:
            random_row, machine_check = upload_file(ns.alive_data_keepers_table, ns.busy_ports_table, machine_check)
        else:
            random_row, machine_check = download_file(
                ns.files_table, ns.alive_data_keepers_table, ns.busy_ports_table, machine_check, file_name)

        # check if there is an empty machine
        if machine_check:
            data_keeper_index = ns.busy_ports_table[
                (ns.busy_ports_table['Data Keeper ID'] == random_row['Data Keeper ID'].iloc[0]) &
                (ns.busy_ports_table['Port'] == random_row['Port'].iloc[0])
            ].index
            busy_port_data_frame = ns.busy_ports_table
            busy_port_data_frame.loc[data_keeper_index, 'Busy'] = True
            ns.busy_ports_table = busy_port_data_frame
            busy_check_lock.release()
            datakeeper_ip = data_keepers_ip[int(random_row['Data Keeper ID'].iloc[0])]
            datakeeper_link = datakeeper_ip + ":" + str(random_row['Port'].iloc[0])
            print("There is an available machine at machine {}".format(datakeeper_link))
            client_socket.send_string(datakeeper_link)
        else:
            busy_check_lock.release()
            print("No empty machine or file does not exist :)")
            client_socket.send_string("")
