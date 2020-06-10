from app.models.genericHandler import GenericHandler
from app.models.models import Message, State
import threading
import time
from random import randint


class AcquireSkeletonHandler(GenericHandler):
    def __init__(self, *, comm, data, state):
        super().__init__(comm=comm, data=data, state=state)

    def _on_state_enter(self):
        self._data.request_timestamp = self._data.timestamp
        self.log(f'Broadcasting REQUEST_SKELETON.', [Message.REQUEST_SKELETON])
        self._broadcast({}, tag=Message.REQUEST_SKELETON)

    def __call__(self, *, msg, status):
        tag = status.Get_tag()
        source = status.Get_source()
        data = self._data

        if tag == Message.ACK_SKELETON:
            data.skeleton_queue_ack += 1
            needed = data.specialist_count - data.skeleton_count
            self.log(f'Got ACK_SKELETON from {source}. Incrementing skeleton_queue_ack to {data.skeleton_queue_ack} of {needed} needed', [
                     Message.ACK_SKELETON])

            if data.skeleton_queue_ack >= needed:
                self.log(f'Got all ACKs. Broadcasting SKELETON_TAKEN. Sending ACK_SKELETON to local queue. Sending START to partners ({data.partners[data.current_job_id]} in job {data.current_job_id})', [
                         Message.ACK_SKELETON, Message.START, Message.SKELETON_TAKEN])
                data.skeleton_count -= 1
                self._broadcast({}, tag=Message.SKELETON_TAKEN)
                self._send_to_targets(
                    {}, targets=data.local_queue, tag=Message.ACK_SKELETON)
                self._send_to_targets(
                    {}, targets=data.partners[data.current_job_id], tag=Message.START)
                self._change_state(State.REVIVING)

        elif tag == Message.REQUEST_SKELETON:
            if data.request_timestamp < msg['timestamp']:
                self.log(
                    f'Got REQUEST_SKELETON from {source}. Adding it to local queue', [Message.REQUEST_SKELETON])
                data.local_queue.append(source)
            elif data.request_timestamp > msg['timestamp']:
                self.log(f'Got REQUEST_SKELETON from {source}, sent ACK_SKELETON',
                         [Message.REQUEST_SKELETON, Message.ACK_SKELETON])
                self._send({}, dest=source, tag=Message.ACK_SKELETON)
            elif data.rank < source:
                self.log(
                    f'Got REQUEST_SKELETON from {source}. Adding it to local queue', [Message.REQUEST_SKELETON])
                data.local_queue.append(source)
            else:
                self.log(f'Got REQUEST_SKELETON from {source}, sent ACK_SKELETON',
                         [Message.REQUEST_SKELETON, Message.ACK_SKELETON])
                self._send({}, dest=source, tag=Message.ACK_SKELETON)

        # respond with acks

        elif tag == Message.REQUEST_JOB:
            job_id = msg['job_id']
            self.log(f'Got REQUEST_JOB from {source} for job_id = {job_id}. Sending ACK_JOB', [
                Message.ACK_JOB, Message.REQUEST_JOB])
            self._send({'job_id': job_id}, dest=source,
                       tag=Message.ACK_JOB)
            if data.specialization == msg['specialization']:
                data.job_map[job_id] = -1

        elif tag == Message.REQUEST_DESK:
            self.log(f'Got REQUEST_DESK from {source}. Responding with ACK_DESK', [
                     Message.REQUEST_DESK, Message.ACK_DESK])
            self._send({}, dest=source, tag=Message.ACK_DESK)
