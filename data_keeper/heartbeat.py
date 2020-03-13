import zmq
import sys
import sched
import time


def init_socket(master_ip, master_heartbeat_port):
    context = zmq.Context.instance()
    master_heartbeat_socket = context.socket(zmq.PUB)
    master_heartbeat_socket.connect('tcp://{0}:{1}'.format(master_ip, master_heartbeat_port))
    return master_heartbeat_socket


def send_heartbeat(scheduler, my_id, master_heartbeat_socket, time_now):
    master_heartbeat_socket.send_string(my_id)
    # print('Heartbeat sent with id {}'.format(my_id))
    scheduler.enterabs(time_now + 1, 0, send_heartbeat,
                       argument=(scheduler, my_id, master_heartbeat_socket, time_now + 1))


def main():
    if len(sys.argv) != 4:
        print('Wrong number of arguments, expected 3')
        sys.exit()
    _, my_id, master_ip, heartbeat_port = sys.argv
    print('Heartbeat process started, sending to master data handler tcp://{}:{}'.format(master_ip, heartbeat_port))
    master_heartbeat_socket = init_socket(master_ip, heartbeat_port)
    scheduler = sched.scheduler(time.time, time.sleep)
    time_now = time.time()
    scheduler.enterabs(time_now + 1, 0, send_heartbeat,
                       argument=(scheduler, my_id, master_heartbeat_socket, time_now + 1))
    scheduler.run()


if __name__ == '__main__':
    main()
