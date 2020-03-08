from datetime import datetime
import zmq
import sys
import pickle
import sched
import time
import numpy as np
import pandas as pd
from multiprocessing import *
import multiprocessing.sharedctypes as sharedctypes
import ctypes
import signal

class TimeoutException(Exception):   # Custom exception class
    pass

def timeout_handler(signum, frame):   # Custom signal handler
    print(" 1 Ssecond had been Passed with no pulses from some machine")
    raise TimeoutException

def whoIsAlive(ns,dataKeeprs,stillAlivePort):
    print(f"Master heartbeat job started, listening to data handlers on port {stillAlivePort}")
    stillAlivePort=int(stillAlivePort)
    dataKeeprs=int(dataKeeprs)

    # initialize Socket to recieve heart beats from data keeprs
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.subscribe('')
    socket.bind ("tcp://*:%s"% stillAlivePort)

    # Periodicly Check who is alive every 1 sec
    while True:

        # Check  who is alive
        aliveListState=[False]*dataKeeprs
        # Check who is alive from datakeeprs every 1 sec
        signal.alarm(1)  
        try:
            for i in range(0,dataKeeprs):
                temp=socket.recv_string()
                temp=int(temp)
                aliveListState[temp]=True
        except TimeoutException:
             # continue the loop if i don't recieve the expected number of data keeprs pulses within 1 sec
            continue
        else:
            # Reset the alarm
            signal.alarm(0)
        print("%d Data Keepers are alive " % sum(aliveListState))
        updatedColoumn = pd.DataFrame({'Alive': aliveListState})
        ns.df2.update(updatedColoumn)