from ..models.models import Message, State
from ..logger.log import log


def requesting_job(comm, msg, status, data):
    tag = status.Get_tag()
    source = status.Get_source()
    job_id = msg['job_id']

    if tag == Message.REJECT_JOB:
        log(
            'Got rejected. Putting -1 into the map. Clearing partners. Changing state to AWAITING_JOB',
            data.state,
            data.rank
        )

        data.job_map[job_id] = -1
        data.partners = []
        data.state = State.AWAITING_JOB

    elif tag == Message.ACK_JOB:
        log('Incrementing job count.', data.state, data.rank)

        data.job_map[job_id] += 1
        if data.job_map[job_id] == data.specialist_count - 1:
            log('Got ACKs from everyone. Sending HELLO to everyone. Changing state to AWAITING_PARTNERS',
                data.state,
                data.rank,
                )

            for proc in range(1, data.specialist_count + 1):
                if proc != data.rank:
                    comm.send({'job_id': job_id},
                              dest=proc, tag=Message.HELLO)
            data.jobs_done += 1
            data.state = State.AWAITING_PARTNERS

    elif tag == Message.HELLO:
        data.partners.append(source)

    elif tag == Message.REQUEST_JOB:
        data.partners.append(source)
