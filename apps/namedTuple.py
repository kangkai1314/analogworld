#--*-- coding:utf-8 --*--
from collections import namedtuple
from functools import partial,wraps
import threading

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

@para_print_log('hello')
def add(x,y,z):
    return x+y+z

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
if __name__ == '__main__':
    main()





