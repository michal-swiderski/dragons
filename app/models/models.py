from enum import IntEnum
from datetime import datetime


class GenericHandler:
    def __init__(self, *, comm, data):
        self.comm = comm
        self.data = data
        self.is_first_call = True

    def __call__(self, *, msg, status):
        pass

    def _change_state(self, target_state):
        self.is_first_call = True
        self.data.state = target_state

    def _send(self, msg, *, dest, tag):
        self.data.timestamp += 1
        msg['specialization'] = self.data.specialization
        msg['timestamp'] = self.data.timestamp
        self.comm.send(msg, dest=dest, tag=tag)

    def _send_to_targets(self, msg, *, targets, tag):
        self.data.timestamp += 1
        msg['specialization'] = self.data.specialization
        msg['timestamp'] = self.data.timestamp

        for target in targets:
            self.comm.send(msg, dest=target, tag=tag)

    def _broadcast(self, msg, *, tag):
        self.data.timestamp += 1
        msg['specialization'] = self.data.specialization
        msg['timestamp'] = self.data.timestamp
        for proc in range(1, self.data.specialist_count + 1):
            if proc != self.data.rank:
                # self._send(msg, dest=proc, tag=tag)
                self.comm.send(msg, dest=proc, tag=tag)

    def _log(self, msg, msg_types=[]):
        # messages_to_check = [Message.REQUEST_DESK, Message.ACK_DESK]
        messages_to_check = []
        now = datetime.now().strftime("%H:%M:%S")
        s = State(self.data.state).name if self.data.state != - \
            1 else 'GENERATOR'
        if len(messages_to_check) == 0 or any(i in messages_to_check for i in msg_types):
            print(
                f'[{now} clock: {self.data.timestamp} TID: {self.data.rank} specialization: {Specialization(self.data.specialization).name} state: {s}] {msg}', end='\n\n')


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
