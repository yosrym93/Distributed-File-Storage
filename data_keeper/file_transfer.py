import zmq
import sys
import pickle

upload = '0'
download = '1'


def init_sockets(master_ip, master_file_transfer_port, file_transfer_port):
    context = zmq.Context.instance()
    file_transfer_socket = context.socket(zmq.PAIR)
    file_transfer_socket.bind('tcp://*:{}'.format(file_transfer_port))
    file_transfer_socket.RCVTIMEO = 10000
    master_data_handler_socket = context.socket(zmq.PUB)
    master_data_handler_socket.connect('tcp://{0}:{1}'.format(master_ip, master_file_transfer_port))
    return file_transfer_socket, master_data_handler_socket


def upload_video(socket, file_name, videos_dir):
    print('Uploading video..')
    try:
        video_data = pickle.loads(socket.recv())
        path = videos_dir + '/' + file_name
        with open(path, 'wb') as video_file:
            video_file.write(video_data)
        success = True
        print('Video uploaded.')
    except zmq.error.Again:
        print('Upload failed. Timeout exceeded.')
        success = False
    return success


def download_video(socket, file_name, videos_dir):
    print('Downloading video..')
    path = videos_dir + '/' + file_name
    with open(path, 'rb') as video_file:
        video_data = video_file.read()
    socket.send(pickle.dumps(video_data))
    print('Video downloaded.')


def start_file_transfer(my_id, file_transfer_socket, master_data_handler_socket, local_file_transfer_port, videos_dir):
    while True:
        try:
            file_name, transfer_type = pickle.loads(file_transfer_socket.recv())
        except zmq.error.Again:
            continue
        if transfer_type == upload:
            success = upload_video(file_transfer_socket, file_name, videos_dir)
            status = {
                'success': success,
                'file_name': file_name,
                'id': my_id,
                'port': local_file_transfer_port
            }
            master_data_handler_socket.send(pickle.dumps(status))
        else:
            download_video(file_transfer_socket, file_name, videos_dir)


def main():
    if len(sys.argv) != 6:
        print('Wrong number of arguments, expected 5')
        sys.exit()
    _, my_id, master_ip, master_file_transfer_port, local_file_transfer_port, videos_dir = sys.argv
    print('File transfer process started, sending to master data handler at tcp://{}:{}, transferring on port {}'
          .format(master_ip, master_file_transfer_port, local_file_transfer_port))
    file_transfer_socket, master_data_handler_socket = \
        init_sockets(master_ip, master_file_transfer_port, local_file_transfer_port)
    start_file_transfer(my_id, file_transfer_socket, master_data_handler_socket, local_file_transfer_port, videos_dir)


if __name__ == '__main__':
    main()
