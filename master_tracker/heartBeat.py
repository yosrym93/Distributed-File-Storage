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

timeout = False

class TimeoutException(Exception):   # Custom exception class
    pass

def timeout_handler(signum, frame):   # Custom signal handler
    global timeout
    timeout = True

def whoIsAlive(ns,dataKeeprs,stillAlivePort):
    time.sleep(2)
    global timeout
    print(f"Master heartbeat job started, listening to data handlers on port {stillAlivePort}")
    stillAlivePort=int(stillAlivePort)
    dataKeeprs=int(dataKeeprs)

    # initialize Socket to recieve heart beats from data keeprs
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.subscribe('')
    socket.bind ("tcp://*:%s"% stillAlivePort)
    # Periodicly Check who is alive every 1 sec

    signal.signal(signal.SIGALRM,timeout_handler)
    aliveListState = [False] * dataKeeprs
    ns.df2 = pd.DataFrame({'Data Keeper ID': range(0, dataKeeprs), 'Alive': aliveListState})
    while True:
        # Check  who is alive
        aliveListState = [False]*dataKeeprs
        # Check who is alive from datakeeprs every 1 sec
        signal.alarm(1)
        while not timeout:
            try:
                temp = socket.recv_string(flags=zmq.NOBLOCK)
                temp = int(temp)
                aliveListState[temp] = True
            except zmq.error.Again:
                continue
        timeout = False
        print(time.time())
        print("%d Data Keepers are alive " % sum(aliveListState))
        df = ns.df2
        df.update({'Alive': aliveListState})
        ns.df2 = df

