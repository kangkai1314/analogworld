#--*-- coding:utf-8 --*--

import datetime,time
import threading
import random
import Queue
from utils.consts import *
from module.life import *

class World:
    def __init__(self,):
        self.time=None
        self.tasklst=[]
        self.status=None
        self.taskevent=threading.Event()
        self.threadlst=[]
        self.currentTask=None
        self.userlst=[]
        self.taskQueue=Queue.Queue()
        self.username='admin'
        self._worldtime=0


    def __repr__(self):
        time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return 'Welcome to {user_name}\'s world at {time}'.format(user_name=self.username,time=time)

    def setSattus(self,status):
        if len(self.tasklst)<=0:
            self.status='啥都不干，你想干啥'
        else:
            self.status='现在正在干活'

    def start(self):
        Mainthread=threading.Thread(target=self.start_world)
        self.threadlst.append(Mainthread)
        Mainthread.start()

    def start_world(self):
        print'请创造你的世界'
        #data=raw_input('请写下最珍贵的是三个人:\n')
        #print data
        while True:
            self.setSattus(0)
            time.sleep(10)
            print 'begin a new life'
            l=Life(self._worldtime)
            real_life=l.initLife()

            self._worldtime=self._worldtime+1
            print self.__repr__()

    def begin_world(self):
        print 'your world is start'
        print 'please choose your life want to expirence'
        data=raw_input('')

    def random_user(self):
        print 'add user'
        age=random.randint(15,30)
        name=random.sample(NameList,1)
        u=User(name[0],age)
        self.userlst.append(u)
        time.sleep(1000)


    def end_world(self):
        pass

    def world_listener(self):
        while True:
            time.sleep(60)


def main():
    world=World()
    print world
    world.start()

if __name__ == '__main__':
    main()