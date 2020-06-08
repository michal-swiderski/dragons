from datetime import datetime

from app.models.models import State


def log(msg, rank, msg_types=[]):
    messages_to_check = []
    now = datetime.now().strftime("%H:%M:%S")
    if len(messages_to_check) == 0 or any(i in messages_to_check for i in msg_types):
        print(f'[{now} TID: {rank}] {msg}')
