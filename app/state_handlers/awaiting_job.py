from ..models.models import Message, State, GenericHandler
from ..logger.log import log


class AwaitingJobHandler(GenericHandler):

    def __init__(self, *, comm, data):
        super().__init__(comm=comm, data=data)

    def __call__(self, *, msg, status):
        data = self.data

        tag = status.Get_tag()
        source = status.Get_source()

        if tag == Message.NEW_JOB:
            job_id = msg['job_id']
            if job_id not in data.job_map:
                data.job_map[job_id] = 0
                data.job_timeout -= 1

                # self._log(data.job_timeout)
                if data.job_timeout < 1:
                    data.job_timeout = 0
                    self.data.current_job_id = job_id
                    self._log(
                        f'Got a job with ID {job_id}, changing state to REQUESTING JOB', [Message.NEW_JOB, Message.REQUEST_JOB])

                    # self._log('CHUJ')
                    self.__send_request_job()
                    self._change_state(State.REQUESTING_JOB)

        elif tag == Message.REQUEST_JOB:
            job_id = msg['job_id']
            specialization = msg['specialization']

            if specialization == data:
                self._send({'job_id': job_id}, dest=status.source,
                           tag=Message.ACK_JOB)
                self._log('Got REQUEST_JOB from the same specialization {status.source}, responding with ACK_JOB. Putting -1 into the map.',
                          [Message.REQUEST_JOB, Message.ACK_JOB])
                data.job_map[job_id] = -1
            else:
                self._send({'job_id': job_id}, dest=status.source,
                           tag=Message.ACK_JOB)
                self._log(f'Got REQUEST_JOB from {status.source}, responding with ACK_JOB',
                          [Message.REQUEST_JOB, Message.ACK_JOB])

                # if data.job_map[job_id] != -1:
                #     self.data.job_map[job_id] = 0
                #     self.data.job_timeout -= 1
                #     self._log(data.job_timeout)
                #     if self.data.job_timeout < 1:
                #         data.job_timeout = 0
                #         self.data.current_job_id = job_id
                #         self.__send_request_job()
                #         self._change_state(State.REQUESTING_JOB)

        elif tag == Message.REQUEST_DESK:
            self._send({}, dest=status.source, tag=Message.ACK_DESK)
            self._log(f'Got REQUEST_DESK from {source}. Sending ACK_DESK', [
                      Message.ACK_DESK, Message.REQUEST_DESK])

        elif tag == Message.REQUEST_SKELETON:
            self._send({},
                       dest=status.source, tag=Message.ACK_SKELETON)
            self._log(f'Got REQUEST_SKELETON from {status.source}, sent ACK_SKELETON', [
                      Message.REQUEST_SKELETON, Message.ACK_SKELETON])

        elif tag == Message.REJECT_JOB:
            if msg['job_id'] == data.last_requested_job:
                data.job_timeout += 1
                self._log(f'Got REJECT_JOB from {source}. Incrementing job_timeout to {data.job_timeout}', [
                          Message.REJECT_JOB])

    def __send_request_job(self):
        self.data.request_timestamp = self.data.timestamp + 1
        self._broadcast({
            'job_id': self.data.current_job_id,
            'jobs_done': self.data.jobs_done
        },
            tag=Message.REQUEST_JOB)
        self._log('Sent REQUEST_JOB to everyone', [Message.REQUEST_JOB])
