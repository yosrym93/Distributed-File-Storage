import zmq
import pandas as pd
import signal

timeout = False


class TimeoutException(Exception):   # Custom exception class
    pass


def timeout_handler(signum, frame):   # Custom signal handler
    global timeout
    timeout = True


def initialize_socket(still_alive_port):
    # initialize Socket to receive heart beats from data keepers
    context = zmq.Context()
    heart_beat_socket = context.socket(zmq.SUB)
    heart_beat_socket.subscribe('')
    heart_beat_socket.bind("tcp://*:%s" % still_alive_port)
    return heart_beat_socket


def who_is_alive(ns, data_keepers_count, still_alive_port):
    global timeout
    print(f"Master heartbeat job started, listening to data handlers on port {still_alive_port}")
    still_alive_port = int(still_alive_port)
    data_keepers_count = int(data_keepers_count)

    # Initialize Heart Beat socket
    heart_beat_socket = initialize_socket(still_alive_port)
    
    # Initialize Signal alarm
    signal.signal(signal.SIGALRM, timeout_handler)

    # Initialize list to keep track of active data keeper
    alive_list_state = [False] * data_keepers_count

    ns.alive_data_keepers_table = pd.DataFrame({'Data Keeper ID': [str(i) for i in range(0, data_keepers_count)],
                                                'Alive': alive_list_state})
    
    # Periodically Check who is alive every 1 sec
    prev = 0
    while True:

        # Check  who is alive
        alive_list_state = [False] * data_keepers_count

        # bind signal every 1 sec
        signal.alarm(1)
        
        while not timeout:
            try:
                data_keeper_id = heart_beat_socket.recv_string(flags=zmq.NOBLOCK)
                data_keeper_id = int(data_keeper_id)
                alive_list_state[data_keeper_id] = True
            except zmq.error.Again:
                continue
        timeout = False
        if prev != sum(alive_list_state):
            print("%d Data Keepers are alive " % sum(alive_list_state))
            prev = sum(alive_list_state)
        
        # Update date frame with data keepers state
        alive_data_frame = ns.alive_data_keepers_table
        alive_data_frame.update({'Alive': alive_list_state})
        ns.alive_data_keepers_table = alive_data_frame
