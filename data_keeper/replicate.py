import zmq
import pickle
import sys

videos_dir = '../videos/'


def init_sockets(master_ip, master_replicate_port, local_replicate_port):
    context = zmq.Context.instance()
    master_replicate_socket = context.socket(zmq.SUB)
    master_replicate_socket.connect('tcp://{0}:{1}'.format(master_ip, master_replicate_port))
    receive_socket = context.socket(zmq.PAIR)
    receive_socket.bind('tcp://*:{}'.format(local_replicate_port))
    send_socket = context.socket(zmq.PAIR)
    return master_replicate_socket, receive_socket, send_socket


def start_replicate_job(my_id, data_keepers_replicate_addresses, master_replicate_socket, send_socket, receive_socket):
    while True:
        replicate_request = pickle.loads(master_replicate_socket.recv())
        if my_id == replicate_request['from']:
            send_video(replicate_request['file_name'], replicate_request['to'],
                       data_keepers_replicate_addresses, send_socket)
        elif my_id in replicate_request['to']:
            receive_video(replicate_request['file_name'], receive_socket)


def send_video(file_name, to, data_keepers_replicate_addresses, socket):
    path = videos_dir + file_name
    with open(path, 'rb') as video_file:
        video_data = video_file.read()
    for data_keeper_id in to:
        address = 'tcp://' + data_keepers_replicate_addresses[data_keeper_id]
        socket.connect(address)
        socket.send(pickle.dumps(video_data))
        socket.close()


def receive_video(file_name, socket):
    video_data = pickle.loads(socket.recv())
    path = videos_dir + file_name
    with open(path, 'wb') as video_file:
        video_file.write(video_data)


def main():
    if len(sys.argv) < 6:
        print('Wrong number of arguments, expected 5')
        sys.exit()
    _, my_id, master_ip, master_replicate_port, local_replicate_port, data_keepers_count = sys.argv[:6]
    if len(sys.argv[6:]) < int(data_keepers_count):
        print('Number of data keepers and addresses provided do not match.')
        sys.exit()
    print('Replicate process started, listening to master replicate job at tcp://{}:{}, receiving on port {}'
          .format(master_ip, master_replicate_port, local_replicate_port))
    data_keepers_replicate_addresses = sys.argv[6:]
    master_replicate_socket, receive_socket, send_socket = \
        init_sockets(master_ip, master_replicate_port, local_replicate_port)
    start_replicate_job(my_id, data_keepers_replicate_addresses, master_replicate_socket, send_socket, receive_socket)


if __name__ == '__main__':
    main()
