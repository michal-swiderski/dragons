from ..models.models import Message, State
from ..logger.log import log


def awaiting_job(*, comm, msg, status, data):
    tag = status.Get_tag()
    job_id = msg['job_id']

    if tag == Message.NEW_JOB:
        if job_id not in data.job_map:
            log(
                f'Got a job with ID {job_id}, changing state to REQUESTING JOB',
                data.state,
                data.rank,
            )
            data.job_map[job_id] = 0
            data.state = State.REQUESTING_JOB

    elif tag == Message.REQUEST_JOB:
        job_id = msg['job_id']
        if msg['specialization'] == data.specialization:
            log('Responding with ACK_JOB. Putting -1 into the map.',
                data.state,
                data.rank,
                )

            data.job_map[job_id] = -1
            comm.send({'job_id': job_id},
                      dest=status.source, tag=Message.ACK_JOB)
        else:
            log(
                'Adding a new job to the map. Changing state to REQUESTING JOB. Responding with ACK_JOB',
                data.state,
                data.rank,
            )

            data.job_map[job_id] = -1
            data.state = State.REQUESTING_JOB
            comm.send({'job_id': job_id},
                      dest=status.source, tag=Message.ACK_JOB)

    elif tag == Message.REQUEST_DESK:
        comm.send({}, dest=status.source, tag=Message.ACK_DESK)

    elif tag == Message.REQUEST_SKELETON:
        comm.send({}, dest=status.source, tag=Message.ACK_SKELETON)
