import zmq
import numpy as np
import pandas as pd
import sys
import pickle


def initialize_busy_port_data_frame(data_keeprs, number_of_process_data_keeper):
    temp=[]
    temp2=[]
    for i in range(0,data_keeprs):
        for j in range(6000,6000+number_of_process_data_keeper):
            temp.append(str(i))
            temp2.append(str(j))
    data = {
        'Port': temp2,
        'Data Keeper ID': temp,
        'Busy': len(temp)*[False]
    }
    return data


def initialize_sockets(successful_check_port, replica_stat_port):
    # Define context to make a socket
    context = zmq.Context()

    # create sockets
    uploaded_success_socket = context.socket(zmq.SUB) 
    replica_success_socket = context.socket(zmq.PULL)

    # set topic to pub/sub model
    uploaded_success_socket.subscribe('')

    # bind sockets
    uploaded_success_socket.bind ("tcp://*:%s"% successful_check_port)
    replica_success_socket.bind("tcp://*:%s"% replica_stat_port)

    return uploaded_success_socket, replica_success_socket


def create_data_frames(data_keeprs,number_process_data_keeper):
    # Create Data Frames
    file_name_data_frame = {
        'Data Keeper ID': [],
        'File Name': [],
        'Is Replicating': []
    }

    return pd.DataFrame(file_name_data_frame),pd.DataFrame(initialize_busy_port_data_frame(data_keeprs, number_process_data_keeper))


def start_master_data_handler(ns, successful_check_port, data_keeprs, number_process_data_keeper,replica_stat_port,
                              busy_check_lock, files_table_lock):
    print('Master data handler started')
    
    # initialize socket
    uploaded_success_socket, replica_success_socket=initialize_sockets(successful_check_port, replica_stat_port)
    
    # initilize table
    ns.files_table, ns.busy_ports_table = create_data_frames(data_keeprs,number_process_data_keeper)

    while True:
        # Check Successful upload
        try:
            stat_upload = pickle.loads(uploaded_success_socket.recv(flags=zmq.NOBLOCK))
            flag = stat_upload['success']
            if flag:
                file_name_data_frame = ns.files_table
                if stat_upload['is_upload']:
                    files_table_lock.acquire()
                    file_name_data_frame = file_name_data_frame.append(
                        {'Data Keeper ID': stat_upload['id'],
                         'File Name': stat_upload['file_name'],
                         'Is Replicating': False},
                        ignore_index=True
                    )
                    files_table_lock.release()
                ns.files_table = file_name_data_frame
                busy_check_lock.acquire()
                data_keeper_id = ns.busy_ports_table[(ns.busy_ports_table['Data Keeper ID'] == stat_upload['id']) &
                                                     (ns.busy_ports_table['Port'] == stat_upload['port'])].index
                busy_port_data_frame = ns.busy_ports_table
                busy_port_data_frame.loc[data_keeper_id,'Busy']=False
                ns.busy_ports_table = busy_port_data_frame
                busy_check_lock.release()
                if stat_upload['is_upload']:
                    print("File Uploaded Successfully")
                else:
                    print("File Downlaod Successfully")
            else:
                print("File Uploaded Unsuccessfully")
        except zmq.error.Again:
            pass
        
        # Check Successful replica
        try:
            stat_replica = pickle.loads(replica_success_socket.recv(flags=zmq.NOBLOCK))
            flag = stat_replica['success']
            if flag:
                files_table_lock.acquire()
                file_name_data_frame = ns.files_table
                index = file_name_data_frame[(file_name_data_frame['Data Keeper ID'] == stat_replica['id']) &
                                             (file_name_data_frame['File Name'] == stat_replica['file_name'])].index
                file_name_data_frame.loc[index, 'Is Replicating'] = False
                ns.files_table = file_name_data_frame
                files_table_lock.release()
                print("File Replicated Successfully")
            else:
                print("File Replicated Unsuccessfully")
        except zmq.error.Again:
            pass
