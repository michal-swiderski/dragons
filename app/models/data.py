from app.models.models import Specialization, State


class Data:
    def __init__(self, rank, skeleton_count, specialist_count, desk_count):
        super().__init__()
        self.rank = rank
        self.specialization = Specialization(rank % 3)
        self.jobs_done = 0
        self.desk_count = desk_count
        self.skeleton_count = skeleton_count
        self.specialist_count = specialist_count
        self.partners = []
        self.job_map = {}
        self.desk_queue_ack = 0
        self.skeleton_queue_ack = 0
        self.job_timeout = 0
        self.last_requested_job = None
        self.timestamp = 1
        self.current_job_id = None
        self.request_timestamp = None
        self.local_queue = []

        self.__state = State.AWAITING_JOB
        self.__callbacks = []

    @property
    def state(self):
        return self.__state

    @property.setter
    def state(self, state):
        self.__state = state
        for cb in self.__callbacks:
            cb()

    def subscribe_to_state_change(self, cb):
        self.__callbacks.append(cb)
