from enum import IntEnum


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
