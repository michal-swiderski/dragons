from ..models.models import Message, State, GenericHandler
from ..logger.log import log


class AwaitingJobHandler(GenericHandler):

    def __init__(self, *, comm, data):
        super().__init__(comm=comm, data=data)

    def __call__(self, *, msg, status):
        data = self.data

        tag = status.Get_tag()
        job_id = msg['job_id']

        if tag == Message.NEW_JOB:
            if job_id not in data.job_map:
                self.__log(f'Got a job with ID {job_id}, changing state to REQUESTING JOB')
                data.job_map[job_id] = 0
                data.job_timeout -= 1
                if data.job_timeout <= 1:
                    data.state = State.REQUESTING_JOB

        elif tag == Message.REQUEST_JOB:
            job_id = msg['job_id']
            if msg['specialization'] == data.specialization:
                self.__log('Responding with ACK_JOB. Putting -1 into the map.')

                data.job_map[job_id] = -1
                # comm.send({'job_id': job_id},
                #           dest=status.source, tag=Message.ACK_JOB)
                self.__send({'job_id': job_id},
                            dest=status.source, tag=Message.ACK_JOB)
            else:
                self.__log('Adding a new job to the map. Changing state to REQUESTING JOB. Responding with ACK_JOB')

                data.job_map[job_id] = -1
                data.state = State.REQUESTING_JOB
                self.__send({'job_id': job_id},
                            dest=status.source, tag=Message.ACK_JOB)

        elif tag == Message.REQUEST_DESK:
            self.__send({}, dest=status.source, tag=Message.ACK_DESK)

        elif tag == Message.REQUEST_SKELETON:
            self.__send({'job_id': msg['job_id']}, dest=status.source, tag=Message.ACK_SKELETON)
