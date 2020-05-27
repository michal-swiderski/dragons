from random import randint
from time import sleep

from app.models.models import Message
from app.logger.log import log


def generator(comm, size):
    JOBS = 2
    last_id = 0
    while last_id < JOBS:
        log(f'Generating a new job with id {last_id}', -1, 0)
        for proc in range(1, size):
            payload = {'job_id': last_id, 'timestamp': 0}
            comm.send(payload, dest=proc, tag=Message.NEW_JOB)
        last_id += 1
        sleep(randint(5, 7))

    log(f'No more jobs will be generated', -1, 0)
