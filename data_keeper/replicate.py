import zmq
import pickle
import sys


def init_sockets(master_ip, master_replicate_port, local_replicate_port, master_notify_port):
    context = zmq.Context.instance()
    master_replicate_socket = context.socket(zmq.SUB)
    master_replicate_socket.subscribe('')
    master_replicate_socket.connect('tcp://{0}:{1}'.format(master_ip, master_replicate_port))
    receive_socket = context.socket(zmq.PAIR)
    receive_socket.bind('tcp://*:{}'.format(local_replicate_port))
    #receive_socket.RCVTIMEO=5000
    send_socket = context.socket(zmq.PAIR)
    master_notify_socket = context.socket(zmq.PUSH)
    master_notify_socket.connect('tcp://{0}:{1}'.format(master_ip, master_notify_port))
    return master_replicate_socket, receive_socket, send_socket, master_notify_socket


def start_replicate_job(my_id, data_keepers_replicate_addresses, master_replicate_socket,
                        videos_dir, send_socket, receive_socket, master_notify_socket):
    while True:
        replicate_request = pickle.loads(master_replicate_socket.recv())
        print('Replicate job requested.')
        if my_id == replicate_request['from']:
            send_video(replicate_request['file_name'], replicate_request['to'],
                       data_keepers_replicate_addresses, send_socket, videos_dir)
        elif my_id in replicate_request['to']:
            success = receive_video(replicate_request['file_name'], receive_socket, videos_dir)
            status = {
                'success': success,
                'file_name': replicate_request['file_name'],
                'id': my_id,
            }
            master_notify_socket.send(pickle.dumps(status))
            print('Notified the master of the replication job.')


def send_video(file_name, to, data_keepers_replicate_addresses, socket, videos_dir):
    print('Starting replicate job, sending video..')
    path = videos_dir + '/' + file_name
    with open(path, 'rb') as video_file:
        video_data = video_file.read()
    for data_keeper_id in to:
        address = 'tcp://' + data_keepers_replicate_addresses[int(data_keeper_id)]
        socket.connect(address)
        socket.send(pickle.dumps(video_data))
        socket.disconnect(address)
    print('Video sent.')


def receive_video(file_name, socket, videos_dir):
    try:
        print('Starting replicate job, receiving video..')
        video_data = pickle.loads(socket.recv())
        path = videos_dir + '/' + file_name
        with open(path, 'wb') as video_file:
            video_file.write(video_data)
        success = True
        print('Video received.')
    except zmq.error.Again:
        print('Upload failed. Timeout exceeded.')
        success = False
    return success


def main():
    if len(sys.argv) < 8:
        print('Wrong number of arguments, expected 7')
        sys.exit()
    _, my_id, master_ip, master_replicate_port, local_replicate_port, master_notify_port, \
        videos_dir, data_keepers_count = sys.argv[:8]
    if len(sys.argv[8:]) < int(data_keepers_count):
        print('Number of data keepers and addresses provided do not match.')
        sys.exit()
    print('Replicate process started, listening to master replicate job at tcp://{}:{}, receiving on port {}'
          .format(master_ip, master_replicate_port, local_replicate_port))
    data_keepers_replicate_addresses = sys.argv[8:]
    master_replicate_socket, receive_socket, send_socket, master_notify_socket = \
        init_sockets(master_ip, master_replicate_port, local_replicate_port, master_notify_port)
    start_replicate_job(my_id, data_keepers_replicate_addresses, master_replicate_socket,
                        videos_dir, send_socket, receive_socket, master_notify_socket)


if __name__ == '__main__':
    main()
