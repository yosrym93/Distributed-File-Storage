import os
import signal
from config import *
from multiprocessing import *
from master_tracker.replica import replica_start
from master_tracker.master_ports import start_client_ports
from master_tracker.heartbeat import who_is_alive
from master_tracker.master_data_handler import start_master_data_handler
from master_tracker.monitor import monitor_data_frame

# Master parameters
# still_alive_port = '5000'
# successfully_check_port = '10000'
# busy_check_port = '9000'
# client_port = 5500
# data_keepers_count = 1
# process_number = 2
# master_replicate_port = '5001'
# replica_factor = 2
# data_keepers_ip = ['25.73.138.51']
# client_ports_count = 2
# replicaPort='8000'


if __name__ == '__main__':
    mgr = Manager()
    ns = mgr.Namespace()

    busy_check_lock=Lock()

    # Create Master data handler process
    data_handler = Process(target=start_master_data_handler,
                           args=(ns, master_file_transfer_port, data_keepers_count, file_transfer_ports_count,
                                 master_replicate_notify_port,busy_check_lock))
    data_handler.start()

    # Creating Who Is Alive Process
    whoIsAliveProcess = Process(target=who_is_alive, args=(ns, str(data_keepers_count), str(master_heartbeat_port)))
    whoIsAliveProcess.start()

    # Creating Replica Process
    replicaProcess = Process(target=replica_start, args=(ns, str(master_replicate_port), str(replica_factor)))
    replicaProcess.start()

    # Creating Ports to Communicate
    for port in range(client_port, client_port + client_ports_count):
        masterPortProcess = Process(target=start_client_ports, args=(str(port), ns, data_keepers_ips, busy_check_lock))
        masterPortProcess.start()

    # Create Moiintor Process that 
    monitor_data_frame=Process(target=monitor_data_frame,args=(ns,))
    monitor_data_frame.start()

    # Kill Processes after pressing any key.
    input('Press any key to exit..\n')
    os.killpg(0, signal.SIGKILL)
