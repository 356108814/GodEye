# encoding: utf-8

from enum import Enum


class State(Enum):
    STARTED = 0
    PAUSED = 1
    STOPPED = 2


class BaseConsumer(object):
    """
    消费者基类
    """

    def __init__(self):
        self._state = State.STOPPED

    @property
    def state(self):
        return self._state

    def start(self):
        self.set_state(State.STARTED)

    def pause(self):
        self.set_state(State.PAUSED)

    def stop(self):
        self.set_state(State.STOPPED)

    def set_state(self, state):
        if isinstance(state, State):
            self._state = state
            if state == State.STARTED:
                self.handle_start()
            elif state == State.PAUSED:
                self.handle_pause()
            elif state == State.STOPPED:
                self.handle_stop()

    def handle_start(self):
        pass

    def handle_pause(self):
        pass

    def handle_stop(self):
        pass

    def consume(self, line_data):
        pass


if __name__ == '__main__':
    print(isinstance(1, State))
