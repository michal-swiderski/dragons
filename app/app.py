from mpi4py import MPI

from app.state_handlers.generator import generator
from app.models.data import Data


def run():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    DESK_COUNT = 10
    SKELETON_COUNT = 10
    SPECIALIST_COUNT = size - 1
    JOBS = 1

    if rank == 0:
        generator.generator(comm, size, JOBS)

    else:
        data = Data(rank, SKELETON_COUNT, SPECIALIST_COUNT, DESK_COUNT)
        # handlers init

        # main loop
        while True:
            status = MPI.Status()
            msg = comm.recv(source=MPI.ANY_SOURCE,
                            tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()

            data.timestamp = max(data.timestamp, msg['timestamp'])
