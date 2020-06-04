from ..models.models import Message, State, GenericHandler


class AwaitingPartnersHandler(GenericHandler):

    def __init__(self, *, comm, data):
        super().__init__(comm=comm, data=data)

    def __call__(self, *, msg, status):
        data = self.data

        tag = status.Get_tag()
        source = status.Get_source()

        if tag == Message.HELLO:
            if msg['job_id'] == data.current_job_id:
                data.partners.append(source)
                self._log(f'Add {source} to partners', [Message.HELLO])
                if len(data.partners) == 2:
                    if data.rank < min(data.partners):
                        self._change_state(State.AWAITING_DESK)
                        self._broadcast({}, tag=Message.REQUEST_DESK)

                        self._log('Changing state to AWAITING_DESK, sending REQUEST_DESK to everyone', [
                                  Message.REQUEST_DESK])
                    else:
                        self._change_state(State.AWAITING_START)
                        self._log('Changing state to AWAITING_START')

        elif tag == Message.REQUEST_DESK:
            self._send({}, dest=source, tag=Message.ACK_DESK)

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
