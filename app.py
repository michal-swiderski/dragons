from mpi4py import MPI
from enum import IntEnum
from time import sleep
from random import randint
import datetime


class Message(IntEnum):
    NEW_JOB = 0
    REQUEST_JOB = 1
    ACK_JOB = 2
    REJECT_JOB = 3
    HELLO = 4
    REQUEST_SKELETON = 5
    ACK_SKELETON = 6
    SKELETON_TAKEN = 7
    START = 8
    FINISH = 9


class State(IntEnum):
    AWAITING_JOB = 0
    REQUESTING_JOB = 1
    AWAITING_PARTNERS = 2
    AWAITING_DESK = 3
    PAPER_WORK = 4
    ACQUIRE_SKELETON = 5
    AWAITING_START = 6
    REVIVNG = 7


class Specialization(IntEnum):
    HEAD = 0
    BODY = 1
    TAIL = 2


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

DESK_COUNT = 100
SKELETON_COUNT = 100
SPECIALIST_COUNT = size - 1

state = -1


def log(msg):
    now = datetime.datetime.now().strftime("%H:%M:%S")
    s = State(state).name if state != -1 else 'GENERATOR'
    print(f'[{now} TID: {rank} state: {s}] {msg}')


if rank == 0:
    last_id = 0
    while True:
        log(f'Generating a new job with id {last_id}')
        for proc in range(1, size):
            payload = {'job_id': last_id}
            comm.send(payload, dest=proc, tag=Message.NEW_JOB)
        last_id += 1
        sleep(randint(5, 7))

else:
    # get a specialization
    specialization = Specialization(rank % 2)
    # start with AWAITING_JOB
    state = State.AWAITING_JOB

    job_map = {}
    partners = []
    desk_queue_ack = 0
    skeleton_queue_ack = 0

    while True:
        status = MPI.Status()
        data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        if state != State.AWAITING_JOB and tag == Message.NEW_JOB:
            if data['job_id'] not in job_map:
                job_map[data['job_id']] = 0

        if state == State.AWAITING_JOB:
            job_id = data['job_id']
            if tag == Message.NEW_JOB:
                if job_id not in job_map:
                    log(f'Got a job with ID {job_id}, changing state to REQUESTING JOB')
                    job_map[job_id] = 0
                    state = State.REQUESTING_JOB
                    continue

            elif tag == Message.REQUEST_JOB:
                job_id = data['job_id']
                if data['specialization'] == specialization:
                    log('Responding with ACK_JOB. Putting -1 into the map.')

                    job_map[job_id] = -1
                    comm.send({'job_id': job_id}, dest=status.source, tag=Message.ACK_JOB)
                else:
                    log('Adding a new job to the map. Changing state to REQUESTING JOB. Responding with ACK_JOB')

                    job_map[job_id] = -1
                    state = State.REQUESTING_JOB
                    comm.send({'job_id': job_id}, dest=status.source, tag=Message.ACK_JOB)

        elif state == State.REQUESTING_JOB:
            job_id = data['job_id']
            if tag == Message.REJECT_JOB:
                log('Got rejected. Putting -1 into the map. Clearing partners. Changing state to AWAITING_JOB')

                job_map[job_id] = -1
                partners = []
                state = State.AWAITING_JOB
            elif tag == Message.ACK_JOB:
                log('Incrementing job count.')

                job_map[job_id] += 1
                if job_map[job_id] == SPECIALIST_COUNT - 1:
                    log('Got ACKs from everyone. Sending HELLO to everyone. Changing state to AWAITING_PARTNERS')
                    for proc in range(1, size):
                        if proc != rank:
                            comm.send({'job_id': job_id}, dest=proc, tag=Message.HELLO)
                    state = State.AWAITING_PARTNERS




