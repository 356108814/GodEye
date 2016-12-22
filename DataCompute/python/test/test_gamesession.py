# encoding: utf-8
"""

@author Yuriseus
@create 16-11-15 上午11:25
"""
from service.minik_server.game_session import GameSessionService

if __name__ == '__main__':
    service = GameSessionService()
    path = '/opt/data-log/minik/logic/2016-11-07.log'
    count = 0
    ds = []
    with open(path, 'r') as f:
        for line in f:
            d = service.split(line)
            if service.filter(d):
                d = service.process_data(d)
                print(d)
