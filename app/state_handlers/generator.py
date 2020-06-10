from time import sleep
from random import randint

from app.models.models import Message


def generator(comm, size, jobs):
    for idx in range(jobs):
        for proc in range(1, size):
            msg = {'job_id': idx, 'timestamp': idx}
            comm.send(msg, dest=proc, tag=Message.NEW_JOB)

        # sleep(randint(0, 5))
