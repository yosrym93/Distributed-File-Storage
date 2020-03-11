import zmq
import pickle
import sched
import time
import numpy as np


def replica(s, ns, replica_factor, replica_socket_to_keepers):
    # Get a table with column values data keeper id, filename and alive status
    print("Replica function started ....")
    merged_table = ns.df.join(ns.df2.set_index('Data Keeper ID'), on='Data Keeper ID')
    # Get names of indexes for which column Alive is False
    index_names = merged_table[merged_table['Alive'] == False].index
    # Delete these row indexes from dataFrame
    merged_table.drop(index_names, inplace=True)
    # Get unique file names
    unique_files = merged_table['File Name'].unique()
    # Get alive data keepers
    # Get names of indexes for which column Alive is False
    index_names = ns.df2[ns.df2['Alive'] == False].index
    # Delete these row indexes from dataFrame
    alive_keepers = ns.df2
    alive_keepers.drop(index_names, inplace=True)
    # Convert alive IDs to numpy array
    alive = alive_keepers['Data Keeper ID']
    alive = np.array(alive)
    unique_files = unique_files.tolist()
    for file in unique_files:
        # Get unique file occurrences in keepers
        file_occurrences = merged_table[merged_table['File Name'] == file]
        # Count the number of replicas
        count = file_occurrences.shape[0]
        # Check the number of replica is smaller than replica factor to replicate it
        if count < replica_factor:
            # Get keepers IDs that the have the file and convert it to numpy array
            occupied = np.array(file_occurrences['Data Keeper ID'])
            # Get free IDs that can replicate the file
            free = np.array(list(filter(lambda x: x not in occupied, alive)))
            # Check if there is machines to replicate.
            if free.size == 0:
                print("No available machines to replicate aborting  ..")
            else:
                # Calculate the number of keepers we need to send to them
                needed_to_be_sent = replica_factor - count
                # Randomize the IDs
                receiver = list(np.random.choice(free, needed_to_be_sent))
                # Send from any source
                sender = occupied[0]
                message = {'from': sender, 'to': receiver, 'file_name': merged_table.iloc[i]['File Name']}
                replica_socket_to_keepers.send(pickle.dumps(message))
    # Run scheduler after another 5 seconds
    s.enter(5, 0, replica, argument=(s, ns, replica_factor,replica_socket_to_keepers))


# Start function 
def replica_start(ns, port_replica, replica_factor):
    print(f"Master replicate job started, sending jobs to data keepers using port {port_replica}")
    # Setup a socket between master and data keepers
    context = zmq.Context()
    # local ip of the master
    replica_socket_to_keepers = context.socket(zmq.PUB)
    replica_socket_to_keepers.bind("tcp://*:" + port_replica)
    # Create scheduler that runs periodically every 5 seconds to check if any file needs to be replicated
    s = sched.scheduler(time.time, time.sleep)
    s.enter(5, 0, replica, argument=(s, ns, int(replica_factor),replica_socket_to_keepers))
    s.run()
