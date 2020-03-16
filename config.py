# Parameters
videos_dir = 'videos'
data_keeper_id = '0'
data_keepers_count = 4
file_transfer_ports_count = 1  # Integer
replica_factor = 2
client_ports_count = 2

# IPs
master_ip = '192.168.43.120'
data_keepers_ips = [
    '192.168.43.120',
    '192.168.43.131',
    '192.168.43.148'
]

# Master ports
master_replicate_port = '5001'
master_heartbeat_port = '5000'
client_port = 5500  # Integer
master_file_transfer_port = '10000'
master_replicate_notify_port = '8000'

# Data keepers ports
file_transfer_ports_start = 6000  # Integer
data_keepers_replicate_port = '7000'
