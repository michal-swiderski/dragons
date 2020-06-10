from app.models.genericHandler import GenericHandler
from app.models.models import Message, State


class AwaitingJobHandler(GenericHandler):
    def __init__(self, *, comm, data, state):
        super().__init__(comm=comm, data=data, state=state)

    def _on_state_enter(self):
        for job in self._data.job_map:
            if self._data.job_map[job] == 0:
                # print(job)
                self._data.request_timestamp = self._data.timestamp
                self._data.current_job_id = job
                self._data.job_timeout = 0

                self.log(
                    f'Changing state to REQUESTING_JOB')
                # self._broadcast(
                #     {'job_id': job, 'jobs_done': self.data.jobs_done}, tag=Message.REQUEST_JOB)
                self._change_state(State.REQUESTING_JOB)
                break

    def __call__(self, *, msg, status):
        tag = status.Get_tag()
        source = status.Get_source()
        data = self._data

        if tag == Message.NEW_JOB:
            job_id = msg['job_id']
            data.job_timeout -= 1
            if data.job_timeout < 1:
                data.job_timeout = 0
                data.current_job_id = job_id

                data.request_timestamp = data.timestamp
                self.log(f'Changing state to REQUESTING_JOB')
                # self._broadcast(
                #     {'job_id': job_id, 'jobs_done': data.jobs_done}, tag=Message.REQUEST_JOB)
                self._change_state(State.REQUESTING_JOB)

        elif tag == Message.REQUEST_JOB:
            if msg['specialization'] == data.specialization:
                data.job_map[msg['job_id']] = -1
                self._send({'job_id': msg['job_id']},
                           dest=source, tag=Message.ACK_JOB)

        elif tag == Message.REJECT_JOB:
            job_id = msg['job_id']
            if data.last_requested_job == job_id:
                data.job_timeout += 1
                self.log()

        elif tag == Message.REQUEST_DESK:
            self.log(f'Got REQUEST_DESK from {source}. Responding with ACK_DESK', [
                     Message.REQUEST_DESK, Message.ACK_DESK])
            self._send({}, dest=source, tag=Message.ACK_DESK)

        elif tag == Message.REQUEST_SKELETON:
            self.log(f'Got REQUEST_SKELETON from {status.source}, sent ACK_SKELETON', [
                Message.REQUEST_SKELETON, Message.ACK_SKELETON])
            self._send({},
                       dest=status.source, tag=Message.ACK_SKELETON)
