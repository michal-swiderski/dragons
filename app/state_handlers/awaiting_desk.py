from ..models.models import Message, State, GenericHandler


class AwaitingDeskHandler(GenericHandler):
    def __init__(self, *, comm, data):
        super().__init__(comm=comm, data=data)

    def __call__(self, *, msg, status):
        data = self.data

        tag = status.Get_tag()
        source = status.Get_source()

        if tag == Message.ACK_DESK:
            data.desk_queue_ack += 1
            needed = data.specialist_count - data.desk_count
            self._log(f'Got ACK_DESK from {source}. Incrementing DESK_QUEUE_ACK to {data.desk_queue_ack} of {needed} needed', [
                      Message.ACK_DESK])
            if data.desk_queue_ack >= needed:
                # sekcja kryt
                self._change_state(State.PAPER_WORK)
                self._log('Changing state to PAPER_WORK')

        elif tag == Message.REQUEST_DESK:
            if self.data.timestamp < msg['timestamp']:
                self.data.local_queue.append(source)
            elif self.data.timestamp < msg['timestamp']:
                self._send({}, source, Message.ACK_DESK)
                self._log(f'Got REQUEST_DESK from {source}, sent ACK_DESK',
                          [Message.REQUEST_DESK, Message.REQUEST_SKELETON])
            elif self.data.rank < source:
                self.data.local_queue.append(source)
            else:
                self._send({}, source, Message.ACK_DESK)
                self._log(f'Got REQUEST_DESK from {source}, sent ACK_DESK',
                          [Message.REQUEST_DESK, Message.REQUEST_SKELETON])

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
