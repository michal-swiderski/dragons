from ..models.models import Message, State, GenericHandler
import threading
import time
from random import randint


class AcquireSkeletonHandler(GenericHandler):

    def __init__(self, *, comm, data):
        super().__init__(comm=comm, data=data)

    def __call__(self, *, msg, status):
        data = self.data

        tag = status.Get_tag()
        source = status.Get_source()

        if tag == Message.ACK_SKELETON:
            data.skeleton_queue_ack += 1
            needed = data.specialist_count - data.skeleton_count
            self._log(
                f'Got ACK_SKELETON from {source}. Incrementing skeleton_queue_ack to {data.skeleton_queue_ack} of {needed} needed')

            if data.skeleton_queue_ack >= needed:
                self._broadcast({}, tag=Message.SKELETON_TAKEN)
                self._send_to_targets(
                    {}, targets=data.local_queue, tag=Message.ACK_SKELETON)
                self._send_to_targets(
                    {}, targets=data.partners, tag=Message.START)
                self._change_state(State.REVIVING)

                data.local_queue = []
                data.skeleton_queue_ack = 0

                threading.Thread(target=self.start_reviving).start()

                self._log('Skeleton taken. Sending SKELETON_TAKEN to everyone, ACK_SKELETON to local queue, START to partners and change state to REVIVING', [
                          Message.ACK_SKELETON, Message.START, Message.SKELETON_TAKEN])

        elif tag == Message.REQUEST_SKELETON:
            if self.data.timestamp < msg['timestamp']:
                self.data.local_queue.append(source)
            elif self.data.timestamp < msg['timestamp']:
                self._send({}, source, Message.ACK_SKELETON)
                self._log(f'Got REQUEST_SKELETON from {source}, sent ACK_SKELETON',
                          [Message.REQUEST_SKELETON, Message.ACK_SKELETON])
            elif self.data.rank < source:
                self.data.local_queue.append(source)
            else:
                self._send({}, source, Message.ACK_SKELETON)
                self._log(f'Got REQUEST_DESK from {source}, sent ACK_SKELETON',
                          [Message.REQUEST_SKELETON, Message.ACK_SKELETON])

        # RESPOND WITH ACKS

        elif tag == Message.REQUEST_DESK:
            self._send({}, dest=source, tag=Message.ACK_DESK)
            self._log(f'Got REQUEST_DESK from {source}. Sending ACK_DESK', [
                Message.ACK_DESK, Message.REQUEST_DESK])

        elif tag == Message.REQUEST_JOB:
            self._send({'job_id': msg['job_id']},
                       dest=source, tag=Message.ACK_JOB)
            self._log(f'Got REQEST_JOB from {source}, sent ACK_JOB', [
                Message.REQUEST_JOB, Message.ACK_JOB])

    def start_reviving(self):
        # sleep_time_range = (1, 5)
        # time.sleep(randint(sleep_time_range[0], sleep_time_range[1]))
        time.sleep(2)

        self._send_to_targets(
            {}, targets=self.data.partners, tag=Message.FINISH)

        self._change_state(State.AWAITING_JOB)
        self.data.partners = []

        self._log('Finished reviving. Send FINISH to partners and change state to AWAITING_JOB', [
                  Message.FINISH])
