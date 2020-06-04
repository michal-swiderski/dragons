from ..models.models import Message, State, GenericHandler
import threading
import time
from random import randint


class PaperWorkHandler(GenericHandler):
    def __init__(self, *, comm, data):
        super().__init__(comm=comm, data=data)

    def __call__(self, *, msg, status):
        data = self.data

        tag = status.Get_tag()
        source = status.Get_source()

        # if self.is_first_call:
        #     threading.Thread(target=self.__do_work, deamon=True)
        #     self.is_first_call = False

        if tag == Message.REQUEST_DESK:
            data.local_queue.append(source)
            self._log(f'Got REQUEST_DESK from {source}. Adding it to local queue', [
                      Message.REQUEST_DESK])

        # RESPOND TO ALL REQUESTS WITH AN ACK MSG

        elif tag == Message.REQUEST_SKELETON:
            self._send({'job_id': msg['job_id']},
                       dest=source, tag=Message.ACK_SKELETON)
            self._log(f'Got REQUEST_SKELETON from {source}, sent ACK_SKELETON', [
                      Message.REQUEST_SKELETON, Message.ACK_SKELETON])

        elif tag == Message.REQUEST_JOB:
            self._send({'job_id': msg['job_id']},
                       dest=source, tag=Message.ACK_JOB)
            self._log(f'Got REQEST_JOB from {source}, sent ACK_JOB', [
                Message.REQUEST_JOB, Message.ACK_JOB])

    # def __do_work(self):
    #     sleep_time_range = (1, 5)
    #     time.sleep(randint(sleep_time_range[0], sleep_time_range[1]))

    #     self._send_to_targets(
    #         {}, targets=self.data.local_queue, tag=Message.ACK_DESK)

    #     self._change_state(State.ACQUIRE_SKELETON)
    #     self.data.local_queue = []

    #     self._broadcast({}, tag=Message.REQUEST_SKELETON)

    #     print('XD')

    #     self._log('sdhjsdakdhsaFinished working. Relasing desk. Sending ACK_DESK to local_queue, REQUEST_SKELETON to all and changing state to ACQUIRE_SKELETON', [
    #               Message.ACK_DESK, Message.REQUEST_SKELETON])
