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
    portColoumn = pd.DataFrame({'Port': temp2})
    idColoumn = pd.DataFrame({'Data Keeper ID': temp})
    busyColoumn = pd.DataFrame({'Busy': len(temp)*[False]})
    return portColoumn,idColoumn,busyColoumn


def start_master_data_handler(ns,successfulCheckPort,busyCheckPort,dataKeeprs,processNumber):

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
    ns.df2 = pd.DataFrame(data2)
    ns.df3 = pd.DataFrame(data3)

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
    portColoumn, idColoumn, busyColoumn = initialize_table(dataKeeprs, processNumber)
    ns.df3.update(portColoumn)
    ns.df3.update(idColoumn)
    ns.df3.update(busyColoumn)


    while(True):
        
        # Check Successful upload
        try:
            stat=pickle.loads(socket2.recv())
            flag=stat['success']
            if flag:
                ns.df.append({'Data Keeper ID':stat['id'],'File Name':stat['file_name']})
                index_name=ns.df3[(ns.df3['Data Keeper ID']==stat['id'])& (ns.df3['Port']==stat['port'])].index
                ns.df3.at[index_name,'Busy']=False
                print("File Uploaded Successfully")
            else:
                print("File Uploaded Unsuccessfully")
        except zmq.error.Again:
            pass

        # Check Busy Ports
        try:
            busyFlag=socket2.recv_pyobj()
            index_name=ns.df3[(ns.df3['Data Keeper ID']==stat['id'])& (ns.df3['Port']==stat['port'])].index
            ns.df3.at[index_name,'Busy']=False
            print("Recieve ",busyFlag[0],busyFlag[1])
        except  zmq.error.Again:
            pass
