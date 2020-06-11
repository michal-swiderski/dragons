from app.models.genericHandler import GenericHandler
from app.models.models import Message, State


class AwaitingDeskHandler(GenericHandler):
    def __init__(self, *, comm, data, state):
        super().__init__(comm=comm, data=data, state=state)

    def _on_state_enter(self):
        self._data.request_timestamp = self._data.timestamp
        self.log(f'Broadcasting REQUEST_DESK.', [Message.REQUEST_DESK])
        self._broadcast({}, tag=Message.REQUEST_DESK)

    def __call__(self, *, msg, status):
        tag = status.Get_tag()
        source = status.Get_source()
        data = self._data

        if tag == Message.ACK_DESK:
            data.desk_queue_ack += 1

            needed = data.specialist_count - data.desk_count
            self.log(f'Got ACK_DESK from {source}. Incrementing DESK_QUEUE_ACK to {data.desk_queue_ack} of {needed} needed', [
                Message.ACK_DESK])
            if data.desk_queue_ack >= needed:
                # crit
                self.log(f'Got all desk ACKs. Changing state to PAPER_WORK.', [
                    Message.ACK_DESK])
                self._change_state(State.PAPER_WORK)

        elif tag == Message.REQUEST_DESK:
            if data.request_timestamp < msg['timestamp']:
                data.local_queue.append(source)
                self.log(f'Got REQUEST_DESK from {source}. Adding it to local queue', [
                         Message.REQUEST_DESK])
            elif data.request_timestamp > msg['timestamp']:
                self.log(f'Got REQUEST_DESK from {source}, sent ACK_DESK',
                         [Message.REQUEST_DESK, Message.REQUEST_SKELETON])
                self._send({}, dest=source, tag=Message.ACK_DESK)
            elif data.rank < source:
                data.local_queue.append(source)
                self.log(f'Got REQUEST_DESK from {source}. Adding it to local queue', [
                         Message.REQUEST_DESK])
            else:
                self.log(f'Got REQUEST_DESK from {source}, sent ACK_DESK',
                         [Message.REQUEST_DESK, Message.REQUEST_SKELETON])
                self._send({}, dest=source, tag=Message.ACK_DESK)

        # respond with acks

        elif tag == Message.REQUEST_JOB:
            job_id = msg['job_id']
            self.log(f'Got REQUEST_JOB from {source} for job_id = {job_id}. Sending ACK_JOB', [
                Message.ACK_JOB, Message.REQUEST_JOB])
            self._send({'job_id': job_id}, dest=source,
                       tag=Message.ACK_JOB)
            if data.specialization == msg['specialization']:
                data.job_map[job_id] = -1

        elif tag == Message.REQUEST_SKELETON:
            self.log(f'Got REQUEST_SKELETON from {status.source}, sent ACK_SKELETON', [
                Message.REQUEST_SKELETON, Message.ACK_SKELETON])
            self._send({},
                       dest=status.source, tag=Message.ACK_SKELETON)
