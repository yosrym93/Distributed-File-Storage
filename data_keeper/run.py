import subprocess
import os
import signal
import shutil


videos_dir = 'videos'
id = '0'
data_keepers_count = '1'
master_ip = '192.168.1.107'
master_replicate_port = '5001'
master_heartbeat_port = '5000'
master_file_transfer_port = '10000'
file_transfer_ports_count = 3     # Integer
file_transfer_ports_start = 6000  # Integer
replicate_port = '7000'
data_keepers_ips = [
    '127.0.0.1',
    '127.0.0.1',
    '127.0.0.1',
    '127.0.0.1'
]

# Create heartbeat process
subprocess.Popen(['python', 'heartbeat.py', id, master_ip, master_heartbeat_port])

# Create replicate process
subprocess.Popen(['python', 'replicate.py', id, master_ip, master_replicate_port, replicate_port, data_keepers_count,
                  *data_keepers_ips])

# Delete videos directory if it exists
try:
    shutil.rmtree(videos_dir)
except OSError as e:
    pass

# Create new videos directory
os.mkdir(videos_dir)

# Create file transfer processes
for port in range(file_transfer_ports_start, file_transfer_ports_start + file_transfer_ports_count):
    subprocess.Popen(['python', 'file_transfer.py', id, master_ip, master_file_transfer_port, str(port), videos_dir])

input('Press any key to exit..\n')
os.killpg(0, signal.SIGKILL)