from mpi4py import MPI
from enum import IntEnum
from time import sleep
from random import randint
# import datetime

# from app.models import Message, State, Specialization
from app.models.models import Message, State, Specialization
from app.logger.log import log
from app.state_handlers import (
    generator,
    requesting_job
)

from app.state_handlers.awaiting_job import AwaitingJobHandler
from app.state_handlers.requesting_job import RequestingJobHandler
from app.state_handlers.awaiting_partners import AwaitingPartnersHandler
from app.state_handlers.awaiting_desk import AwaitingDeskHandler


class Data:
    def __init__(self, rank, skeleton_count, specialist_count, desk_count):
        super().__init__()
        self.rank = rank
        self.specialization = Specialization(rank % 3)
        self.jobs_done = 0
        self.state = State.AWAITING_JOB
        self.desk_count = desk_count
        self.skeleton_count = skeleton_count
        self.specialist_count = specialist_count
        self.partners = []
        self.job_map = {}
        self.desk_queue_ack = 0
        self.skeleton_queue_ack = 0
        self.job_timeout = 0
        self.last_requested_job = None
        self.timestamp = 1
        self.current_job_id = None
        self.request_timestamp = None
        self.local_queue = []


def run():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    DESK_COUNT = 1
    SKELETON_COUNT = 100
    SPECIALIST_COUNT = size - 1

    if rank == 0:
        generator.generator(comm, size)

    else:
        data = Data(rank, SKELETON_COUNT, SPECIALIST_COUNT, DESK_COUNT)

        awaiting_job_handler = AwaitingJobHandler(comm=comm, data=data)
        requesting_job_handler = RequestingJobHandler(comm=comm, data=data)
        awaiting_partners_handler = AwaitingPartnersHandler(
            comm=comm, data=data)
        awaiting_desk_handler = AwaitingDeskHandler(comm=comm, data=data)

        while True:
            status = MPI.Status()
            msg = comm.recv(source=MPI.ANY_SOURCE,
                            tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()

            data.timestamp = max(data.timestamp, msg['timestamp'])
            if data.state != State.AWAITING_JOB and tag == Message.NEW_JOB:
                if msg['job_id'] not in data.job_map:
                    data.job_map[msg['job_id']] = 0

            if data.state == State.AWAITING_JOB:
                awaiting_job_handler(msg=msg, status=status)

            elif data.state == State.REQUESTING_JOB:
                requesting_job_handler(msg=msg, status=status)

            elif data.state == State.AWAITING_PARTNERS:
                awaiting_partners_handler(msg=msg, status=status)

            elif data.state == State.AWAITING_DESK:
                awaiting_desk_handler(msg=msg, status=status)
