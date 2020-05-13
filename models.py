from enum import IntEnum


class Message(IntEnum):
    NEW_JOB = 0
    REQUEST_JOB = 1
    ACK_JOB = 2
    REJECT_JOB = 3
    HELLO = 4
    REQUEST_SKELETON = 5
    ACK_SKELETON = 6
    SKELETON_TAKEN = 7
    START = 8
    FINISH = 9


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
