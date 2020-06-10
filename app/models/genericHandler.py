from datetime import datetime

from app.models.data import Data
from app.models.models import State, Specialization, Message


class GenericHandler:
    def __init__(self, *, comm, data, state):
        self._comm = comm
        self._data = data
        self.__self_state = state

        self._data.subscribe_to_state_change(self.__on_state_change)

    def __on_state_change(self, new_state):
        if new_state == self.__self_state:
            self._on_state_enter()

    def _on_state_enter(self):
        pass

    def __on_before_state_exit(self):
        pass

    def _change_state(self, target_state):
        self.__on_before_state_exit()
        self._data.state = target_state

    def _send(self, msg, *, dest, tag):
        self._data.timestamp += 1
        msg['specialization'] = self._data.specialization
        msg['timestamp'] = self._data.timestamp
        self._comm.send(msg, dest=dest, tag=tag)

    def _broadcast(self, msg, *, tag):
        data = self._data
        data.timestamp += 1
        msg['specialization'] = data.specialization
        msg['timestamp'] = data.timestamp
        for proc in range(1, data.specialist_count + 1):
            if proc != data.rank:
                self._comm.send(msg, dest=proc, tag=tag)

    def _send_to_targets(self, msg, *, targets, tag):
        self._data.timestamp += 1
        msg['specialization'] = self._data.specialization
        msg['timestamp'] = self._data.timestamp

        for target in targets:
            self._comm.send(msg, dest=target, tag=tag)

    def log(self, msg, msg_types=[]):
        data = self._data
        messages_to_check = []
        messages_to_check = [Message.REQUEST_SKELETON,
                             Message.ACK_SKELETON, Message.SKELETON_TAKEN]

        tids_to_check = []
        # tids_to_check = [3]

        now = datetime.now().strftime("%H:%M:%S")
        s = State(data.state).name if data.state != - \
            1 else 'GENERATOR'
        if len(messages_to_check) == 0 or any(i in messages_to_check for i in msg_types):
            if len(tids_to_check) == 0 or data.rank in tids_to_check:
                print(
                    f'[{now} clock: {data.timestamp} TID: {data.rank} spec: {Specialization(data.specialization).name} state: {s}] {msg}', end='\n\n')
