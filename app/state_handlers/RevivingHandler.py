from time import sleep
from random import randint
import threading

from app.models.genericHandler import GenericHandler
from app.models.models import Message, State


class RevivingHandler(GenericHandler):
    def __init__(self, *, comm, data, state):
        super().__init__(comm=comm, data=data, state=state)

    def _on_state_enter(self):
        def revive():
            sleep(randint(1, 5))
            self.log(f'Finished reviving. Sending FINISH to partners. Changing state to AWAITING_JOB.', [
                Message.FINISH])
            self._send_to_targets(
                {}, targets=self._data.partners[self._data.current_job_id], tag=Message.FINISH)
            self.__reset()
            self._change_state(State.AWAITING_JOB)

        if self._data.rank < min(self._data.partners[self._data.current_job_id]):
            threading.Thread(target=revive, daemon=True).start()
            # print(self._data.rank)

    def __call__(self, *, msg, status):
        tag = status.Get_tag()
        source = status.Get_source()
        data = self._data

        if tag == Message.FINISH:
            self.log(f'Got FINISH from {source}. Changing state to AWAITING_JOB', [
                     Message.FINISH])
            self.__reset()
            self._change_state(State.AWAITING_JOB)

        elif tag == Message.REQUEST_DESK:
            self.log(f'Got REQUEST_DESK from {source}. Sending ACK_DESK', [
                Message.ACK_DESK, Message.REQUEST_DESK])
            self._send({}, dest=source, tag=Message.ACK_DESK)

        elif tag == Message.REQUEST_SKELETON:
            self.log(f'Got REQUEST_SKELETON from {source}, sent ACK_SKELETON', [
                Message.REQUEST_SKELETON, Message.ACK_SKELETON])
            self._send({}, dest=source, tag=Message.ACK_SKELETON)

        elif tag == Message.REQUEST_JOB:
            job_id = msg['job_id']
            self.log(f'Got REQEST_JOB from {source}, sent ACK_JOB', [
                Message.REQUEST_JOB, Message.ACK_JOB])
            self._send({'job_id': msg['job_id']},
                       dest=source, tag=Message.ACK_JOB)
            if data.specialization == msg['specialization']:
                data.job_map[job_id] = -1

    def __reset(self):
        data = self._data

        data.job_map[data.current_job_id] = -1
        data.current_job_id = None
        data.last_job_id = None
        data.skeleton_queue_ack = 0
        data.desk_queue_ack = 0
        data.jobs_done += 1
        data.local_queue = []
        data.request_timestamp = None
