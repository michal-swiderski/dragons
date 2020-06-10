from app.models.data import Data


class GenericHandler:
    def __init__(self, *, comm, data, state):
        self._comm = comm
        self._data = data
        self.__self_state = state

        self.data.subscribe_to_state_change(self.__on_state_enter)

    def __on_state_enter(self, new_state):
        if new_state == self.__self_state:
            self._on_state_enter()

    def _on_state_enter(self):
        pass

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

    def _change_state(self, target_state):
        self._data.state = target_state
