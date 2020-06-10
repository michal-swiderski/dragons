from app.models.genericHandler import GenericHandler
from app.models.models import Message, State
import threading
import time
from random import randint


class PaperWorkHandler(GenericHandler):
    def __init__(self, *, comm, data, state):
        super().__init__(comm=comm, data=data, state=state)

    def _on_state_enter(self):
        def do_paperwork():
            # time.sleep(randint(1, 5))
            time.sleep(1)

            self.log('Done doing paperwork. Sending ACK_DESK to local queue. Changing state to ACQUIRE SKELETON', [
                     Message.ACK_DESK])

            self._send_to_targets(
                {}, targets=self._data.local_queue, tag=Message.ACK_DESK)
            self._data.local_queue = []

            self._change_state(State.ACQUIRE_SKELETON)

        threading.Thread(target=do_paperwork, daemon=True).start()

    def __call__(self, *, msg, status):
        tag = status.Get_tag()
        source = status.Get_source()
        data = self._data

        if tag == Message.REQUEST_JOB:
            job_id = msg['job_id']
            self.log(f'Got REQUEST_JOB from {source} for job_id = {job_id}. Sending ACK_JOB', [
                Message.ACK_JOB, Message.REQUEST_JOB])
            self._send({'job_id': job_id}, dest=source,
                       tag=Message.ACK_JOB)

        elif tag == Message.REQUEST_DESK:
            self.log(f'Got REQUEST_DESK from {source}. Responding with ACK_DESK', [
                     Message.REQUEST_DESK, Message.ACK_DESK])
            self._send({}, dest=source, tag=Message.ACK_DESK)

        elif tag == Message.REQUEST_SKELETON:
            self._send({},
                       dest=status.source, tag=Message.ACK_SKELETON)
            self._log(f'Got REQUEST_SKELETON from {status.source}, sent ACK_SKELETON', [
                      Message.REQUEST_SKELETON, Message.ACK_SKELETON])
