import subprocess
import os
import signal
from multiprocessing import *
from replica import replica_start
from master_ports import start_client_ports
from heartBeat import whoIsAlive
from masterDataHandler import start_master_data_handler


# Master parameters
still_alive_port = '5000'
successfully_check_port = '10000'
busy_check_port = '9000'
client_port = 5500
data_keepers_count = 2
process_number = 3
master_replicate_port = '5001'
replica_factor = 1
data_keepers_ip = ["192.168.43.120:"]
client_ports_count = 1


if __name__ == '__main__':
    mgr = Manager()
    ns = mgr.Namespace()

    # Create Master data handler process
    data_handler = Process(target=start_master_data_handler,
                           args=(ns, successfully_check_port, busy_check_port, data_keepers_count, process_number))
    data_handler.start()

    # Creating Who Is Alive Process
    whoIsAliveProcess = Process(target=whoIsAlive, args=(ns, str(data_keepers_count), str(still_alive_port)))
    whoIsAliveProcess.start()

    # Creating Replica Process
    replicaProcess = Process(target=replica_start, args=(ns, str(master_replicate_port), str(replica_factor)))
    replicaProcess.start()

    # Creating Ports to Communicate
    for port in range(client_port, client_port + client_ports_count):
        masterPortProcess = Process(target=start_client_ports, args=(str(port), busy_check_port, ns, data_keepers_ip))
        masterPortProcess.start()


    # Kill Processes after pressing any key.
    input('Press any key to exit..\n')
    os.killpg(0, signal.SIGKILL)
