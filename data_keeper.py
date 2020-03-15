import subprocess
import os
import signal
import shutil
from config import *


if __name__ == '__main__':
    data_keepers_replicate_addresses = [ip + ':' + data_keepers_replicate_port for ip in data_keepers_ips]

    # Create heartbeat process
    subprocess.Popen(['python', 'data_keeper/heartbeat.py', data_keeper_id, master_ip, master_heartbeat_port])

    # Create replicate process
    subprocess.Popen(
        ['python', 'data_keeper/replicate.py', data_keeper_id, master_ip, master_replicate_port,
         data_keepers_replicate_port, master_replicate_notify_port, videos_dir, str(data_keepers_count),
         *data_keepers_replicate_addresses])

    # Delete videos directory if it exists
    try:
        shutil.rmtree(videos_dir)
    except OSError as e:
        pass

    # Create new videos directory
    os.mkdir(videos_dir)

    # Create file transfer processes
    for port in range(file_transfer_ports_start, file_transfer_ports_start + file_transfer_ports_count):
        subprocess.Popen(
            ['python', 'data_keeper/file_transfer.py', data_keeper_id, master_ip, master_file_transfer_port, str(port), videos_dir])

    input('Press any key to exit..\n')
    os.killpg(0, signal.SIGKILL)
