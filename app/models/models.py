from enum import IntEnum
from datetime import datetime


class GenericHandler:
    def __init__(self, *, comm, data):
        self.comm = comm
        self.data = data

    def __call__(self, *, msg, status):
        pass

    def _send(self, msg, *, dest, tag):
        self.comm.send(msg, dest=dest, tag=tag)
        self.data.lamport += 1

    def _broadcast(self, msg, tag):
        for proc in range(1, self.data.specialist_count + 1):
            if proc != self.data.rank:
                self._send(msg, dest=proc, tag=tag)

    def _log(self, msg):
        now = datetime.now().strftime("%H:%M:%S")
        s = State(self.data.state).name if self.data.state != -1 else 'GENERATOR'
        print(f'[{now} TID: {self.data.rank} state: {s}] {msg}')


class Message(IntEnum):
    NEW_JOB = 0
    REQUEST_JOB = 1
    ACK_JOB = 2
    REJECT_JOB = 3
    HELLO = 4
    REQUEST_DESK = 5
    ACK_DESK = 6
    RELEASE_DESK = 7
    REQUEST_SKELETON = 8
    ACK_SKELETON = 9
    SKELETON_TAKEN = 10
    START = 11
    FINISH = 12


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
