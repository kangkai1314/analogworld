#--*-- coding:utf-8 --*--
from collections import namedtuple
from functools import partial,wraps
import threading
import datetime,time
import inspect
from threading import current_thread
import cProfile

class Logger():
    def __init__(self):
        self.lock=threading.RLock()

    def prints(self,msg):
        self.lock.acquire()
        print msg
        self.lock.release()


class NamedTuple(tuple):

    def __new__(cls, iterable, fileds):
        if hasattr(cls,iterable):
            pass



    def __init__(self,iterable,fileds):
        self.iterable=iterable
        self.fileds=fileds


    def __repr__(self):
        return 'NamedTuple'

    def init(self):
        for key,i in enumerate(self.iterable):
            print key,i


class Call(object):
    def __call__(self, *args, **kwargs):
        print
        return 'call function'


class DoResult():
    def __init__(self):
        self.result = {}


def do_add(result,key):

    ret='hello'
    result[key]=ret
    return 'hello'

class Do(DoResult):
    def __init__(self):
        DoResult.__init__(self)
        self.threadlst=[]

    def run(self):
        for i in range(5):
            t=threading.Thread(target=do_add,args=(self.result,'2'))
            self.threadlst.append(t)
        for i in self.threadlst:
            print i.name
            i.run()

        while self.threadlst !=[]:
            for i in self.threadlst:

                if i.isAlive:
                    print ''
                else:
                    print '%s is stopped' % (i.name)
                    self.threadlst.remove(i)

        print self.threadlst
        return self.result



def print_log(func):

    @wraps(func)
    def wrapper(*args,**kwargs):
        print 'this is start'
        ret=func(*args,**kwargs)
        return ret
    return wrapper
def para_print_log(msg):
    def decorator(func):
        @wraps(func)
        def wrapper(*args,**kwargs):
            print 'this is start %s'%(msg)
            ret=func(*args,**kwargs)
            return ret
        return wrapper
    return decorator


def self_para_print_log(func):
        @wraps(func)
        def wrapper(self,*args,**kwargs):
            print 'this is start %s'%(self.msg)
            ret=func(self,*args,**kwargs)
            return ret
        return wrapper




@para_print_log('hello')
def add(x,y,z):
    return x+y+z



class Msg(object):
    msg='hello'
    def __init__(self):
        self.msg='test'
    @self_para_print_log
    def add(self):
        return True

class ExtMsg(Msg):
    def __init__(self):
        super(Msg,self).__init__()





class Threadmanager(threading.Thread):
    def __init__(self,arg):
        super(Threadmanager,self).__init__()
        self.arg=arg

    def get_current_class(self):
        return inspect.stack()[1][3]

    def run(self):
        print 'get current class '
        print self.get_current_class()
        print self.name
        time.sleep(10)
        if self.isAlive():
            print '%s is running'%(self.name)

class Tmanager(object):
    def __init__(self,threadlst):
        self.threadlst=threadlst

    def start(self):
        t=threading.Thread(target=self.run)
        t.start()
        t.join()

    def run(self):
        for t in self.threadlst:
            t.start()

        while self.threadlst!=[]:
            for t in self.threadlst:
                if t.isAlive():
                    continue
                else:
                    self.threadlst.remove(t)
            print 'current threads %s'%(str(self.threadlst))
            time.sleep(10)

global resultdict
resultdict={}


def sleeps(sec,result,lock):
    t=threading.current_thread()
    print t.name
    lock.acquire()
    print 'add lock'
    global sum
    sum=0
    time.sleep(sec)
    sum+=sec
    resultdict[str(sec)] = sum
    lock.release()
    print 'release lock'

def produceThreads():
    threadlst=[]
    lock=threading.RLock()
    for i in range(10):
        t=threading.Thread(target=sleeps,args=(i+1,resultdict,lock))
        threadlst.append(t)
    return threadlst


def main():
    #d=Do()
   # print d.run()
    a=partial(add,100)
    b=a(5,6)
    print b
    dict1={'12':'23'}
    print {v:k for k,v in dict1.items()}
    h=add(1,2,3)
    print h
    m=Msg()
    m.add()
    t1=time.clock()

    time.sleep(10)
    t2=time.clock()
    cost=t2-t1
    print cost
    print type(cost)
    s=Threadmanager('add')
    print Threadmanager.__name__
    s.run()
    tlst=produceThreads()
    tm=Tmanager(tlst)
    tm.run()
    print resultdict
    key='1'
    ls='hello'
    print
    extmsg=ExtMsg()
    #extmsg.add()
    cProfile.run(extmsg.add())
    ll='1.2.3.4.5'
    [i for i in range(len(ll)]






if __name__ == '__main__':
    main()