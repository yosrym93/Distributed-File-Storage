import subprocess
import os
import signal
import pandas as pd
from multiprocessing import *
from .replica import replica_start
from .master_ports import start_client_ports
from .heartBeat import whoIsAlive


# Master parameters
still_alive_port = '5000'
successfully_check_port = '10000'
busy_check_port = ''
client_port = 5500
data_keepers = int
process_number = int
master_replicate_port = '5001'
replica_factor = 2
data_keepers_ip = []
master_client_ports_count = int


if __name__ == '__main__':
    mgr = Manager()
    ns = mgr.Namespace()

    # Create Master data handler process
    subprocess.Popen(
        ['python', 'masterDataHandler.py', ns, successfully_check_port, busy_check_port, data_keepers, process_number]
    )

    # Creating Who Is Alive Process
    whoIsAliveProcess = Process(target=whoIsAlive, args=(ns, str(data_keepers), str(still_alive_port)))
    whoIsAliveProcess.start()

    # Creating Replica Process
    replicaProcess = Process(target=replica_start, args=(ns, str(master_replicate_port), str(replica_factor)))
    replicaProcess.start()

    # Creating Ports to Communicate
    for i in range(master_client_ports_count):
        masterPortProcess = Process(target=start_client_ports, args=("192.168.1.107:" + str(client_port+i),
                                                                     busy_check_port, ns))
        masterPortProcess.start()


    # Kill Processes after pressing any key.
    input('Press any key to exit..\n')
    os.killpg(0, signal.SIGKILL)
