from mpi4py import MPI
from enum import IntEnum
from time import sleep
from random import randint
# import datetime

from models import Message, State, Specialization
from log import log
from generator import generator

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

DESK_COUNT = 100
SKELETON_COUNT = 100
SPECIALIST_COUNT = size - 1

state = -1

if rank == 0:
    generator(comm, size)

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
                    log(
                        f'Got a job with ID {job_id}, changing state to REQUESTING JOB', state, rank)
                    job_map[job_id] = 0
                    state = State.REQUESTING_JOB
                    continue

            elif tag == Message.REQUEST_JOB:
                job_id = data['job_id']
                if data['specialization'] == specialization:
                    log('Responding with ACK_JOB. Putting -1 into the map.', state, rank)

                    job_map[job_id] = -1
                    comm.send({'job_id': job_id},
                              dest=status.source, tag=Message.ACK_JOB)
                else:
                    log('Adding a new job to the map. Changing state to REQUESTING JOB. Responding with ACK_JOB', state, rank)

                    job_map[job_id] = -1
                    state = State.REQUESTING_JOB
                    comm.send({'job_id': job_id},
                              dest=status.source, tag=Message.ACK_JOB)

        elif state == State.REQUESTING_JOB:
            job_id = data['job_id']
            if tag == Message.REJECT_JOB:
                log('Got rejected. Putting -1 into the map. Clearing partners. Changing state to AWAITING_JOB', state, rank)

                job_map[job_id] = -1
                partners = []
                state = State.AWAITING_JOB
            elif tag == Message.ACK_JOB:
                log('Incrementing job count.', state, rank)

                job_map[job_id] += 1
                if job_map[job_id] == SPECIALIST_COUNT - 1:
                    log('Got ACKs from everyone. Sending HELLO to everyone. Changing state to AWAITING_PARTNERS', state, rank)
                    for proc in range(1, size):
                        if proc != rank:
                            comm.send({'job_id': job_id},
                                      dest=proc, tag=Message.HELLO)
                    state = State.AWAITING_PARTNERS
