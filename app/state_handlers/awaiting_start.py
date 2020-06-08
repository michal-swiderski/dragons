from ..models.models import Message, State, GenericHandler


class AwaitingStartHandler(GenericHandler):

    def __init__(self, *, comm, data):
        super().__init__(comm=comm, data=data)

    def __call__(self, *, msg, status):
        data = self.data

        tag = status.Get_tag()
        source = status.Get_source()

        if tag == Message.START:
            self._change_state(State.REVIVING)
            self._log('Got START, changing state to REVIVING', [Message.START])

        elif tag == Message.REQUEST_DESK:
            self._send({}, dest=source, tag=Message.ACK_DESK)
            self._log(f'Got REQUEST_DESK from {source}. Sending ACK_DESK', [
                Message.ACK_DESK, Message.REQUEST_DESK])

        elif tag == Message.REQUEST_SKELETON:
            self._send({}, dest=source, tag=Message.ACK_SKELETON)
            self._log(f'Got REQUEST_SKELETON from {source}, sent ACK_SKELETON', [
                Message.REQUEST_SKELETON, Message.ACK_SKELETON])

        elif tag == Message.REQUEST_JOB:
            self._send({'job_id': msg['job_id']},
                       dest=source, tag=Message.ACK_JOB)
            self._log(f'Got REQEST_JOB from {source}, sent ACK_JOB', [
                Message.REQUEST_JOB, Message.ACK_JOB])
            if msg['specialization'] == data.specialization:
                data.job_map[msg['job_id']] = -1
