from ..models.models import Message, State, GenericHandler


class RequestingJobHandler(GenericHandler):
    def __init__(self, *, comm, data):
        super().__init__(comm=comm, data=data)

    def __call__(self, *, msg, status):
        data = self.data

        tag = status.Get_tag()
        source = status.Get_source()

        if tag == Message.REJECT_JOB:
            data.job_map[msg['job_id']] = -1
            data.partners[msg['job_id']] = []
            data.last_requested_job = data.current_job_id
            data.job_timeout += 1
            self._change_state(State.AWAITING_JOB)

            self._log(
                f'Got REJECT_JOB from {source}. Putting -1 into the map. Incrementing job_timeout to {data.job_timeout}. Clearing partners. Changing state to AWAITING_JOB and searching fo jobs', [Message.REJECT_JOB])

            self._check_for_jobs()

        elif tag == Message.ACK_JOB:
            job_id = msg['job_id']
            data.job_map[job_id] += 1
            self._log(f'Got ACK_JOB from {status.source}. Incrementing ack count for {data.current_job_id} to {data.job_map[job_id]} of {data.specialist_count - 1} needed.', [
                      Message.ACK_JOB])

            if data.job_map[job_id] == data.specialist_count - 1:
                self._broadcast({'job_id': job_id}, tag=Message.HELLO)
                self._log(
                    'Got ACKs from everyone. Sending HELLO to everyone. Changing state to AWAITING_PARTNERS',
                    [Message.ACK_JOB, Message.HELLO])

                data.jobs_done += 1
                # if data.rank == 1:
                #     print(data.rank, data.partners, job_id)
                # self._log(f'{data.rank}, {data.partners}, {job_id}')
                if len(data.partners[job_id]) == 2:
                    if data.rank < min(data.partners[job_id]):
                        self._change_state(State.AWAITING_DESK)
                        self._broadcast({}, tag=Message.REQUEST_DESK)
                    else:
                        self._change_state(State.AWAITING_START)
                else:
                    self._change_state(State.AWAITING_PARTNERS)

        # elif tag == Message.HELLO:
        #     if msg['job_id'] == data.current_job_id:
        #         data.partners.append(source)
        #         self._log(f'Add {source} to partners', [Message.HELLO])

        elif tag == Message.REQUEST_JOB:
            job_id = msg['job_id']
            specialization = msg['specialization']

            if job_id != self.data.current_job_id:
                self._send({'job_id': job_id}, dest=source,
                           tag=Message.ACK_JOB)
                self._log(f'Got a REQUEST_JOB from {source}. Sending ACK_JOB', [
                          Message.ACK_JOB, Message.REQUEST_JOB])
                if specialization == self.data.specialization:
                    self.data.job_map[job_id] = -1

            else:
                if specialization != self.data.specialization:
                    self._send({'job_id': job_id}, dest=source,
                               tag=Message.ACK_JOB)
                    self._log(
                        f'Got a REQUEST_JOB from {source}. Sending ACK_JOB', [Message.ACK_JOB, Message.REQUEST_JOB])
                else:
                    has_priority = self.__has_priority(
                        msg['timestamp'], msg['jobs_done'], source)
                    if has_priority:
                        self._send({'job_id': job_id}, dest=source,
                                   tag=Message.REJECT_JOB)
                        self._log(
                            f'Got a REQUEST_JOB from {source}. Sending REJECT_JOB', [Message.REJECT_JOB, Message.REQUEST_JOB])
                    else:
                        self._send({'job_id': job_id}, dest=source,
                                   tag=Message.ACK_JOB)
                        self._log(
                            f'Got a REQUEST_JOB from {source}. Sending ACK_JOB', [Message.ACK_JOB, Message.REQUEST_JOB])

        elif tag == Message.REQUEST_DESK:
            self._send({}, dest=source, tag=Message.ACK_DESK)
            self._log(f'Got REQUEST_DESK from {source}. Sending ACK_DESK', [
                      Message.ACK_DESK, Message.REQUEST_DESK])

        elif tag == Message.REQUEST_SKELETON:
            self._send({},
                       dest=source, tag=Message.ACK_SKELETON)
            self._log(f'Got REQUEST_SKELETON from {source}, sent ACK_SKELETON', [
                Message.REQUEST_SKELETON, Message.ACK_SKELETON])

    def __has_priority(self, source_timestamp, source_jobs_done, source_rank):
        '''Returns true if I have priority'''
        # if self.data.rank % 3 == source_rank % 3:
        #     print(self.data.rank, [self.data.request_timestamp, self.data.jobs_done, self.data.rank],
        #           [source_timestamp, source_jobs_done, source_rank])
        if self.data.jobs_done < source_jobs_done:
            return True
        elif self.data.jobs_done > source_jobs_done:
            return False
        elif self.data.request_timestamp < source_timestamp:
            return True
        elif self.data.request_timestamp > source_timestamp:
            return False
        elif self.data.rank < source_rank:
            return True
        else:
            return False
