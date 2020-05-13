from random import randint
from time import sleep

from log import log
from models import Message


def generator(comm, size):
    last_id = 0
    while True:
        log(f'Generating a new job with id {last_id}', -1, 0)
        for proc in range(1, size):
            payload = {'job_id': last_id}
            comm.send(payload, dest=proc, tag=Message.NEW_JOB)
        last_id += 1
        sleep(randint(5, 7))
