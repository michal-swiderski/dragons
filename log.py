from datetime import datetime

from models import State


def log(msg, state, rank):
    now = datetime.now().strftime("%H:%M:%S")
    s = State(state).name if state != -1 else 'GENERATOR'
    print(f'[{now} TID: {rank} state: {s}] {msg}')
