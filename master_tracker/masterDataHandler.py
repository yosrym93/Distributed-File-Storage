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
            temp2.append(j)
    data = {
        'Port': temp2,
        'Data Keeper ID': temp,
        'Busy': len(temp)*[False]
    }
    return data

def initialize_sockets(successful_check_port,busy_check_port,replica_stat_port):

    # Define context to make a socket
    context = zmq.Context()

    #create sockets
    uploaded_success_socket = context.socket(zmq.SUB) 
    check_busy_socket = context.socket(zmq.PULL)
    replica_success_socket = context.socket(zmq.PULL)

    #set topic to pub/sub model
    uploaded_success_socket.subscribe('')

    #bind sockets
    uploaded_success_socket.bind ("tcp://*:%s"% successful_check_port)
    check_busy_socket.bind("tcp://*:%s"% busy_check_port)
    replica_success_socket.bind("tcp://*:%s"% replica_stat_port)

    return uploaded_success_socket,check_busy_socket,replica_success_socket

def create_data_frames(data_keeprs,number_process_data_keeper):
    
     # Create Data Frames
    file_name_data_frame = {
        'Data Keeper ID': [],
        'File Name': []
    }

    return pd.DataFrame(file_name_data_frame),pd.DataFrame(initialize_busy_port_data_frame(data_keeprs, number_process_data_keeper))

def start_master_data_handler(ns, successful_check_port, busy_check_port, data_keeprs, number_process_data_keeper,replica_stat_port):
    
    print(f"Master data handler started, listening to busy checks on port {busy_check_port}")
    
    # initialize socket
    uploaded_success_socket,check_busy_socket,replica_success_socket=initialize_sockets(successful_check_port,busy_check_port,replica_stat_port)
    
    # initilize table
    ns.df ,ns.df3= create_data_frames(data_keeprs,number_process_data_keeper)

    while(True):
        
        # Check Successful upload
        try:
            stat_upload=pickle.loads(uploaded_success_socket.recv(flags=zmq.NOBLOCK))
            flag=stat_upload['success']
            if flag:
                file_name_data_frame = ns.df
                file_name_data_frame = file_name_data_frame.append({'Data Keeper ID':stat_upload['id'],'File Name':stat_upload['file_name']},ignore_index=True)
                ns.df = file_name_data_frame
                data_keeper_id=ns.df3[(ns.df3['Data Keeper ID']==stat_upload['id'])& (ns.df3['Port']==stat_upload['port'])].index
                busy_port_data_frame = ns.df3
                busy_port_data_frame.at[data_keeper_id,'Busy']=False
                ns.df3 = busy_port_data_frame
                print("File Uploaded Successfully")
            else:
                print("File Uploaded Unsuccessfully")
        except zmq.error.Again:
            pass
        
        # Check Successful replica
        try:
            stat_replica=pickle.loads(replica_success_socket.recv(flags=zmq.NOBLOCK))
            flag2=stat_replica['success']
            if flag2:
                file_name_data_frame = ns.df
                file_name_data_frame = file_name_data_frame.append({'Data Keeper ID':stat_replica['id'],'File Name':stat_replica['file_name']},ignore_index=True)
                ns.df = file_name_data_frame
                print("File Replicated Successfully")
            else:
                print("File Replicated Unsuccessfully")
        except zmq.error.Again:
            pass

        # Check Busy Ports
        try:
            busy_data_keeper=check_busy_socket.recv_pyobj(flags=zmq.NOBLOCK)
            data_keeper_id=ns.df3[(ns.df3['Data Keeper ID']==busy_data_keeper[0])& (ns.df3['Port']==busy_data_keeper[1])].index
            busy_port_data_frame = ns.df3
            busy_port_data_frame.at[data_keeper_id,'Busy']=False
            ns.df3 = busy_port_data_frame
        except zmq.error.Again:
            pass
