from app.models.genericHandler import GenericHandler
from app.models.models import Message, State


class RequestingJobHandler(GenericHandler):
    def __init__(self, *, comm, data, state):
        super().__init__(comm=comm, data=data, state=state)

    def _on_state_enter(self):
        self._data.request_timestamp = self._data.timestamp
        self.log(
            f'Sending REQUEST_JOBs for job_id = {self._data.current_job_id}', [Message.REQUEST_JOB])
        self._broadcast({'job_id': self._data.current_job_id, 'jobs_done': self._data.jobs_done},
                        tag=Message.REQUEST_JOB)

    def __call__(self, *, msg, status):
        tag = status.Get_tag()
        source = status.Get_source()
        data = self._data

        if tag == Message.REJECT_JOB:
            job_id = msg['job_id']
            data.job_map[job_id] = -1
            data.last_requested_job = job_id
            data.current_job_id = None

            self.log(f'Got REJECT_JOB from {source}. Changing state to AWAITING_JOB', [
                     Message.REJECT_JOB])
            self._change_state(State.AWAITING_JOB)

        elif tag == Message.ACK_JOB:
            job_id = msg['job_id']
            if job_id == data.current_job_id:
                data.job_map[job_id] += 1
                self.log(f'Got ACK_JOB from {status.source}. Incrementing ack count for job_id = {data.current_job_id} to {data.job_map[job_id]} of {data.specialist_count - 1} needed.', [
                    Message.ACK_JOB])
                if data.job_map[job_id] == data.specialist_count - 1:
                    self.log(f'Received all job ACKs. Broadcasting HELLO.', [
                        Message.HELLO])
                    self._broadcast({'job_id': job_id}, tag=Message.HELLO)
                    if len(data.partners[job_id]) == 2:
                        if data.rank < min(data.partners[job_id]):
                            self.log('Changing state to AWAITING_DESK')
                            self._change_state(State.AWAITING_DESK)
                        else:
                            self.log('Changing state to AWAITING_START')
                            self._change_state(State.AWAITING_START)
                    else:
                        self.log('Changing state to AWAITING_PARTNERS')
                        self._change_state(State.AWAITING_PARTNERS)

        elif tag == Message.REQUEST_JOB:
            job_id = msg['job_id']
            if data.current_job_id != job_id:
                self.log(f'Got REQUEST_JOB from {source} for job_id = {job_id}. Sending ACK_JOB', [
                         Message.ACK_JOB, Message.REQUEST_JOB])
                self._send({'job_id': job_id}, dest=source,
                           tag=Message.ACK_JOB)
                if data.specialization == msg['specialization']:
                    data.job_map[job_id] = -1

            else:
                if data.specialization != msg['specialization']:
                    self.log(f'Got REQUEST_JOB from {source} for job_id = {job_id}. Sending ACK_JOB', [
                        Message.ACK_JOB, Message.REQUEST_JOB])
                    self._send({'job_id': job_id}, dest=source,
                               tag=Message.ACK_JOB)
                else:
                    has_priority = self.__has_priority(
                        msg['timestamp'], msg['jobs_done'], source)
                    if has_priority:
                        self.log(f'Got REQUEST_JOB from {source} for job_id = {job_id}. Sending REJECT_JOB', [
                                 Message.REJECT_JOB, Message.REQUEST_JOB])
                        self._send({'job_id': job_id}, dest=source,
                                   tag=Message.REJECT_JOB)
                    else:
                        self.log(f'Got REQUEST_JOB from {source} for job_id = {job_id}. Sending ACK_JOB', [
                            Message.ACK_JOB, Message.REQUEST_JOB])
                        self._send({'job_id': job_id}, dest=source,
                                   tag=Message.ACK_JOB)

        elif tag == Message.REQUEST_DESK:
            self.log(f'Got REQUEST_DESK from {source}. Responding with ACK_DESK', [
                     Message.REQUEST_DESK, Message.ACK_DESK])
            self._send({}, dest=source, tag=Message.ACK_DESK)

        elif tag == Message.REQUEST_SKELETON:
            self.log(f'Got REQUEST_SKELETON from {status.source}, sent ACK_SKELETON', [
                Message.REQUEST_SKELETON, Message.ACK_SKELETON])
            self._send({}, dest=status.source, tag=Message.ACK_SKELETON)

    def __has_priority(self, source_timestamp, source_jobs_done, source_rank):
        '''Returns true if I have priority'''
        # if self.data.rank % 3 == source_rank % 3:
        # print(self._data.rank, [self._data.request_timestamp, self._data.jobs_done, self._data.rank],
        #       [source_timestamp, source_jobs_done, source_rank])
        if self._data.jobs_done < source_jobs_done:
            return True
        elif self._data.jobs_done > source_jobs_done:
            return False
        elif self._data.request_timestamp + 1 < source_timestamp:
            return True
        elif self._data.request_timestamp + 1 > source_timestamp:
            return False
        elif self._data.rank < source_rank:
            return True
        else:
            return False
