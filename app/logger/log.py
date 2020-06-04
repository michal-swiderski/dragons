from datetime import datetime

from app.models.models import State


def log(msg, rank, msg_types=[]):
    messages_to_check = []
    now = datetime.now().strftime("%H:%M:%S")
    if len(messages_to_check) == 0 or any(i in messages_to_check for i in msg_types):
        print(f'[{now} TID: {rank}] {msg}')


def _log(self, msg, msg_types=[]):
    # messages_to_check = [Message.REQUEST_DESK, Message.ACK_DESK]
    messages_to_check = []
    now = datetime.now().strftime("%H:%M:%S")
    s = State(self.data.state).name if self.data.state != - \
        1 else 'GENERATOR'
    if len(messages_to_check) == 0 or any(i in messages_to_check for i in msg_types):
        print(
            f'[{now} clock: {self.data.timestamp} TID: {self.data.rank} specialization: {Specialization(self.data.specialization).name} state: {s}] {msg}', end='\n\n')
