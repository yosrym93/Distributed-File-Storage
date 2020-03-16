import zmq
import sys
import pickle
import random

upload = '0'
download = '1'


def master_connection(context, master_ip, file_name, transfer_mode, port_list):
    master_socket = context.socket(zmq.REQ)
    for port in port_list:
        master_link = master_ip + ":" + str(port)
        master_socket.connect("tcp://{link}".format(link=master_link))
        print('Connected to {}'.format(master_link))

    master_socket.send_pyobj((file_name, transfer_mode))
    print("Request sent to master data handler")
    # receive port from master
    data_keeper_link = master_socket.recv_string()
    print("data keeper link: " + data_keeper_link)
    return data_keeper_link


def data_keeper_connection(context, data_keeper_link):
    data_keeper_socket = context.socket(zmq.PAIR)
    data_keeper_socket.connect("tcp://{link}".format(link=data_keeper_link))
    return data_keeper_socket


def upload_file(data_keeper_socket, file_name):
    with open(file_name, "rb") as video_file:
        video = video_file.read()
    data_keeper_socket.send(pickle.dumps(video))
    return


def download_file(data_keeper_socket, file_name):
    video = pickle.loads(data_keeper_socket.recv())
    with open(file_name, "wb") as video_file:
        video_file.write(video)
    return


def main():
    _, master_ip, master_port, ports_count, file_name, transfer_mode = sys.argv
    context = zmq.Context()

    # connect to all master ports randomly
    port_list = list(range(int(master_port), int(master_port) + int(ports_count)))
    random.shuffle(port_list)
    data_keeper_link = master_connection(context, master_ip, file_name, transfer_mode, port_list)

    if not data_keeper_link:
        print('No empty ports on the server or file does not exist')
        return
    data_keeper_socket = data_keeper_connection(context, data_keeper_link)
    # send request to data keeper
    data_keeper_socket.send(pickle.dumps((file_name, transfer_mode)))
    if transfer_mode == upload:
        upload_file(data_keeper_socket, file_name)
    else:
        download_file(data_keeper_socket, file_name)
    data_keeper_socket.disconnect("tcp://{link}".format(link=data_keeper_link))


if __name__ == '__main__':
    main()
