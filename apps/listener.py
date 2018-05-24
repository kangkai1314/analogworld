#--*-- coding:utf-8 --*--
from itertools import groupby
import time,threading
import Queue
import datetime
from threading import current_thread
from collections import defaultdict


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

def main():
    threadlst=[]
    for i in range(5):
        t=threading.Thread(target=sleep,args=(10,))
        threadlst.append(t)
    tl=ThreadListener(threadlst)
    ll=iter(tl)
    print ll
    for i in ll :
        print i

    for i in threadlst:
        print i
        i.start()

    for i in threadlst:
        print i
        i.join()
    tl.run()







 


if __name__ == '__main__':
    main()
