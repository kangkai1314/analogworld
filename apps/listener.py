#--*-- coding:utf-8 --*--
from itertools import groupby
import time,threading
import Queue
import datetime
from threading import current_thread
from collections import defaultdict
from adapter import BaseAdapter
import random

import concurrent
from concurrent import futures

class BaseListener(object):
    type='default'

    def __init__(self):
        pass

    @classmethod
    def getListener(cls):
        return cls.type

    def __str__(self):
        return 'this is defalut listener'

    def __repr__(self):
        pass

    def run(self):
        raise NotImplementedError

    def close(self):
        pass


    def __call__(self,arg=None):
        print '将一个类的实例转换为函数调用'
        print 'call listener'


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print exc_type,exc_val,exc_tb


class ThreadListener(BaseListener):
    type='thread'

    def __init__(self,threadlst):
        super(BaseListener,self).__init__()
        self.threadlst=threadlst
        self.resultqueue=Queue.Queue()

    def __iter__(self):
        return (i for i in self.threadlst)

    def run(self,):
        time.sleep(10)
        while True:
            start_time=datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            for t in self.threadlst:
                print t.name
                print t.isAlive()
                if not t.isAlive():
                    self.threadlst.remove(t)
            time.sleep(2)
            if self.threadlst==[]:
                break
    def __del__(self):
        self.threadlst=None

class TaskListener(BaseListener):
    type='task'
    def __init__(self):
        super(BaseListener,self).__init__()


class EventListener(BaseListener):
    type='event'
    def __init__(self):
        super(BaseListener,self).__init__()
        self.eventlst=[]

    def run(self):
        while True:
            for e in self.eventlst:
                status=e.status
                if status:
                    self.eventlst.remove()





def sleep(n):
    lock=threading.RLock()
    lock.acquire()
    start=time.clock()
    print current_thread().name
    time.sleep(n)
    end=time.clock()
    print '%s cost %s s'%(current_thread().name,str(end-start))
    lock.release()



class ChatRobot(object):
    def __init__(self):
        pass

    def chat(self):

        answer=None
        while True:
            question = yield answer
            answer =self.gen_answer()
            print '康凯:{}'.format(question)



    def gen_answer(self):
        answerlst=['1','2','3']
        return random.choice(answerlst)



def  download_one(pics):
    print ' start dowmload '




    def rmdup(list1, list2):
        print list1
        print list2
        sort_list1 = []

def queue(gropups):
    if gropups.find('GB') >-1 or gropups.find('BG')>-1:
        pass
    else:
        return 0

def find(t,a):
    for al in a:

        for j in al:
            print j
            if t ==j:
                return True
            else:
                return  False


def replace(str):
    str1=''
    for i in str.split():
        gen_str=i+'%20'
        str1+=gen_str

    return str1




def main():
    cr=ChatRobot()
    chat=cr.chat()
    next(chat)
    questionlst=['你最喜欢的人?','你最喜欢的动物是什么','你最喜欢的球星是什么']
    for i in questionlst:
        ans=chat.send(i)
        print '林志玲:{}'.format(ans)

    a=[1,2,3,4,5]
    print a[::2]
    print a[-2:]
    print a[:-2]
    print sum(i+3 for i in a[::2])
    find(7,[[1,2,8,9],[2,4,9,12],[4,7,10,13],[6,8,11,15]])
    find(5,[[1,2,8,9],[2,4,9,12],[4,7,10,13],[6,8,11,15]])
    print replace('WE ARE HAPPY')







if __name__ == '__main__':
    main()
