from mpi4py import MPI

from app.state_handlers.generator import generator
from app.models.data import Data
from app.models.models import Message, State

from app.state_handlers.AwaitingJobHandler import AwaitingJobHandler


def run():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    DESK_COUNT = 10
    SKELETON_COUNT = 10
    SPECIALIST_COUNT = size - 1
    JOBS = 1

    if rank == 0:
        generator(comm, size, JOBS)

    else:
        data = Data(rank, SKELETON_COUNT, SPECIALIST_COUNT, DESK_COUNT)
        # handlers init
        awaiting_job_handler = AwaitingJobHandler(
            comm=comm, data=data, state=State.AWAITING_JOB)
        # main loop
        data.state = State.AWAITING_JOB
        while True:
            status = MPI.Status()
            msg = comm.recv(source=MPI.ANY_SOURCE,
                            tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()
            source = status.Get_source()

            data.timestamp = max(data.timestamp, msg['timestamp']) + 1

            if tag == Message.NEW_JOB:
                data.job_map[msg['job_id']] = 0
                data.partners[msg['job_id']] = []
                awaiting_job_handler.log(
                    f'Got NEW_JOB with job_id = {msg["job_id"]}', [Message.NEW_JOB])
            elif tag == Message.HELLO:
                data.partners[msg['job_id']].append(source)
                awaiting_job_handler.log(
                    f'Got HELLO from {source} for job_id = {msg["job_id"]}', [Message.HELLO])
            elif tag == Message.SKELETON_TAKEN:
                data.skeleton_count -= 1
                awaiting_job_handler.log(f'Got SKELETON_TAKEN from {source}. Decrementing skeleton count to {data.skeleton_count}', [
                                         Message.SKELETON_TAKEN])

            if data.state == State.AWAITING_JOB:
                awaiting_job_handler(msg=msg, status=status)
            elif data.state == State.REQUESTING_JOB:
                pass
            elif data.state == State.AWAITING_PARTNERS:
                pass
            elif data.state == State.AWAITING_DESK:
                pass
            elif data.state == State.PAPER_WORK:
                pass
            elif data.state == State.ACQUIRE_SKELETON:
                pass
            elif data.state == State.AWAITING_START:
                pass
            elif data.state == State.REVIVING:
                pass
