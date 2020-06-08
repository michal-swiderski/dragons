from ..models.models import Message, State, GenericHandler


class RevivingHandler(GenericHandler):
    def __init__(self, *, comm, data):
        super().__init__(comm=comm, data=data)

    def __call__(self, *, msg, status):
        data = self.data
        tag = status.Get_tag()
        source = status.Get_source()

        if tag == Message.FINISH:
            self._log(f'Got FINISH from {source}. Changing state to AWAITING_JOB', [
                      Message.FINISH])
            data.job_map[data.last_requested_job] = -1
            data.partners = []
            data.desk_queue_ack = 0
            data.skeleton_queue_ack = 0
            data.job_timeout = 0
            data.last_requested_job = None
            data.current_job_id = None
            data.local_queue = []

            self._change_state(State.AWAITING_JOB)

        # RESPOND WITH ACKs

        elif tag == Message.REQUEST_DESK:
            self._send({}, dest=source, tag=Message.ACK_DESK)
            self._log(f'Got REQUEST_DESK from {source}. Sending ACK_DESK', [
                Message.ACK_DESK, Message.REQUEST_DESK])

        elif tag == Message.REQUEST_SKELETON:
            self._send({},
                       dest=source, tag=Message.ACK_SKELETON)
            self._log(f'Got REQUEST_SKELETON from {source}, sent ACK_SKELETON', [
                      Message.REQUEST_SKELETON, Message.ACK_SKELETON])

        elif tag == Message.REQUEST_JOB:
            self._send({'job_id': msg['job_id']},
                       dest=source, tag=Message.ACK_JOB)
            self._log(f'Got REQEST_JOB from {source}, sent ACK_JOB', [
                Message.REQUEST_JOB, Message.ACK_JOB])
