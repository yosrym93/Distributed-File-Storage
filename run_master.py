import os
import signal
from config import *
from multiprocessing import *
from master_tracker.replica import replica_start
from master_tracker.master_ports import start_client_ports
from master_tracker.heartbeat import who_is_alive
from master_tracker.master_data_handler import start_master_data_handler
from master_tracker.monitor import monitor_data_frame


if __name__ == '__main__':
    mgr = Manager()
    ns = mgr.Namespace()

    busy_check_lock = Lock()
    files_table_lock = Lock()

    # Create Master data handler process
    data_handler = Process(target=start_master_data_handler,
                           args=(ns, master_file_transfer_port, data_keepers_count, file_transfer_ports_count,
                                 master_replicate_notify_port, busy_check_lock, files_table_lock))
    data_handler.start()

    # Creating Who Is Alive Process
    whoIsAliveProcess = Process(target=who_is_alive, args=(ns, str(data_keepers_count), str(master_heartbeat_port)))
    whoIsAliveProcess.start()

    # Creating Replica Process
    replicaProcess = Process(target=replica_start,
                             args=(ns, str(master_replicate_port), str(replica_factor), files_table_lock))
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
