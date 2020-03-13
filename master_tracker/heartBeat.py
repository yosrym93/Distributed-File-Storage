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

def initialize_socket(still_alive_port):
    # initialize Socket to recieve heart beats from data keeprs
    context = zmq.Context()
    heart_beat_socket = context.socket(zmq.SUB)
    heart_beat_socket.subscribe('')
    heart_beat_socket.bind ("tcp://*:%s"% still_alive_port)
    return heart_beat_socket


def whoIsAlive(ns,data_keeprs,still_alive_port,heart_beat_lock):
    
    global timeout
    print(f"Master heartbeat job started, listening to data handlers on port {still_alive_port}")
    still_alive_port=int(still_alive_port)
    data_keeprs=int(data_keeprs)

    # Initialize Heart Beat socket
    heart_beat_socket=initialize_socket(still_alive_port)
    
    #Initialize Signal alarm
    signal.signal(signal.SIGALRM,timeout_handler)

    #Initilalize list to keep track of active data keeper
    alive_list_state = [False] * data_keeprs

    ns.df2 = pd.DataFrame({'Data Keeper ID': [str(i) for i in range(0, data_keeprs)], 'Alive': alive_list_state})
    
    # Periodicly Check who is alive every 1 sec
    prev=0
    while True:

        # Check  who is alive
        alive_list_state = [False]*data_keeprs

        # bind signal every 1 sec
        signal.alarm(2)
        
        while not timeout:
            try:
                data_keeprs_id = heart_beat_socket.recv_string(flags=zmq.NOBLOCK)
                data_keeprs_id = int(data_keeprs_id)
                alive_list_state[data_keeprs_id] = True
            except zmq.error.Again:
                continue
        timeout = False
        if prev != sum(alive_list_state):
            print("%d Data Keepers are alive " % sum(alive_list_state))
            prev=sum(alive_list_state)
        
        # Update date frame with datakeeprs state
        heart_beat_lock.acquire()
        alive_data_frame = ns.df2
        alive_data_frame.update({'Alive': alive_list_state})
        ns.df2 = alive_data_frame
        heart_beat_lock.release()

