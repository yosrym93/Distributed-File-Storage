import zmq
import numpy as np
import pandas as pd
import sys
import pickle


def initialize_table(dataKeeprs, processNumber):
    temp=[]
    temp2=[]
    for i in range(0,dataKeeprs):
        for j in range(6000,6000+processNumber):
            temp.append(i)
            temp2.append(j)
    data = {
        'Port': temp2,
        'Data Keeper ID': temp,
        'Busy': len(temp)*[False]
    }
    return data


def start_master_data_handler(ns, successfulCheckPort, busyCheckPort, dataKeeprs, processNumber):
    print(f"Master data handler started, listening to busy checks on port {busyCheckPort}")
    # Create Data Frames
    data = {
        'Data Keeper ID': [],
        'File Name': []
    }

    data2 = {
        'Data Keeper ID': [],
        'Alive': []
    }

    data3 = {
        'Data Keeper ID': [],
        'Port': [],
        'Busy': []
    }

    # initilize table
    ns.df = pd.DataFrame(data)
    ns.df3 = pd.DataFrame(initialize_table(dataKeeprs, processNumber))

    # Socket to talk to server
    context = zmq.Context()

    socket2 = context.socket(zmq.SUB) #need to be push / pull 
    socket3 = context.socket(zmq.PULL)

    socket2.subscribe('')

    socket2.bind ("tcp://*:%s"% successfulCheckPort)
    socket2.setsockopt(zmq.RCVTIMEO, 500)

    socket3.bind("tcp://*:%s"% busyCheckPort)
    socket3.setsockopt(zmq.RCVTIMEO, 500)

    # Fill the table with data

    while(True):
        
        # Check Successful upload
        try:
            stat=pickle.loads(socket2.recv())
            flag=stat['success']
            if flag:
                df = ns.df
                df.append({'Data Keeper ID':stat['id'],'File Name':stat['file_name']})
                ns.df = df
                index_name=ns.df3[(ns.df3['Data Keeper ID']==stat['id'])& (ns.df3['Port']==stat['port'])].index
                df = ns.df3
                df.at[index_name,'Busy']=False
                ns.df3 = df
                print("File Uploaded Successfully")
            else:
                print("File Uploaded Unsuccessfully")
        except zmq.error.Again:
            pass

        # Check Busy Ports
        try:
            busyFlag=socket2.recv_pyobj()
            index_name=ns.df3[(ns.df3['Data Keeper ID']==stat['id'])& (ns.df3['Port']==stat['port'])].index
            df = ns.df3
            df.at[index_name,'Busy']=False
            ns.df3 = df
            print("Recieve ",busyFlag[0],busyFlag[1])
        except  zmq.error.Again:
            pass
