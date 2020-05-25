from ..models.models import Message, State, GenericHandler


class RequestingJobHandler(GenericHandler):
    def __init__(self, *, comm, data):
        super().__init__(comm=comm, data=data)

    def __call__(self, *, msg, status):
        data = self.data

        tag = status.Get_tag()
        source = status.Get_source()

        # current_job = data.job_map[]
        # self._broadcast({'job_id' : })

        if tag == Message.REJECT_JOB:
            self._log('Got rejected. Putting -1 into the map. Clearing partners. Changing state to AWAITING_JOB')

            data.job_map[msg['job_id']] = -1
            data.partners = []
            data.state = State.AWAITING_JOB

        elif tag == Message.ACK_JOB:
            job_id = msg['job_id']
            self._log('Incrementing job count.')

            data.job_map[job_id] += 1
            if data.job_map[job_id] == data.specialist_count - 1:
                self._log('Got ACKs from everyone. Sending HELLO to everyone. Changing state to AWAITING_PARTNERS')

                for proc in range(1, data.specialist_count + 1):
                    if proc != data.rank:
                        self._send({'job_id': job_id}, dest=proc, tag=Message.HELLO)
                data.jobs_done += 1
                data.state = State.AWAITING_PARTNERS

        elif tag == Message.HELLO:
            data.partners.append(source)

        elif tag == Message.REQUEST_JOB:
            job_id = msg['job_id']

            if job_id not in self.data.job_map:
                self._send({'job_id': job_id}, dest=status.source, tag=Message.ACK_JOB)
