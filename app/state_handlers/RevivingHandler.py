from app.models.genericHandler import GenericHandler
from app.models.models import Message, State


class RevivingHandler(GenericHandler):
    def __init__(self, *, comm, data, state):
        super().__init__(comm=comm, data=data, state=state)

    def _on_state_enter(self):
        if self._data.rank < min(self._data.partners[self._data.current_job_id]):
            print(self._data.rank)

    def __call__(self, *, msg, status):
        tag = status.Get_tag()
        source = status.Get_source()
        data = self._data

        if tag == Message.REQUEST_DESK:
            self.log(f'Got REQUEST_DESK from {source}. Sending ACK_DESK', [
                Message.ACK_DESK, Message.REQUEST_DESK])
            self._send({}, dest=source, tag=Message.ACK_DESK)

        elif tag == Message.REQUEST_SKELETON:
            self.log(f'Got REQUEST_SKELETON from {source}, sent ACK_SKELETON', [
                Message.REQUEST_SKELETON, Message.ACK_SKELETON])
            self._send({}, dest=source, tag=Message.ACK_SKELETON)

        elif tag == Message.REQUEST_JOB:
            self.log(f'Got REQEST_JOB from {source}, sent ACK_JOB', [
                Message.REQUEST_JOB, Message.ACK_JOB])
            self._send({'job_id': msg['job_id']},
                       dest=source, tag=Message.ACK_JOB)
