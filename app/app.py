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
    awaiting_job,
    requesting_job
)


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


def run():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    DESK_COUNT = 100
    SKELETON_COUNT = 100
    SPECIALIST_COUNT = size - 1

    # state = -1

    if rank == 0:
        generator.generator(comm, size)

    else:
        data = Data(rank, SKELETON_COUNT, SPECIALIST_COUNT, DESK_COUNT)

        while True:
            status = MPI.Status()
            msg = comm.recv(source=MPI.ANY_SOURCE,
                            tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()

            if data.state != State.AWAITING_JOB and tag == Message.NEW_JOB:
                if msg['job_id'] not in data.job_map:
                    data.job_map[msg['job_id']] = 0

            if data.state == State.AWAITING_JOB:
                awaiting_job.awaiting_job(comm, msg, status, data)

            elif data.state == State.REQUESTING_JOB:
                requesting_job.requesting_job(comm, msg, status, data)