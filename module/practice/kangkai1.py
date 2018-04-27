#--*-- coding:utf-8 --*--
import string
from functools import  wraps
from collections import OrderedDict,defaultdict


def parse_size(s):
    def mult(multiplier):
        return int(s[:-1])*multiplier

    if all(x in string.digits for x in s):
        return int(s)
    if s.endswith('b'):
        return mult(1)
    if s.endswith('k'):
        return mult(1024)
    if s.endswith('m'):
        return mult(1024*1024)
    if s.endswith('g'):
        return mult(1024*1024*1024)
    raise Exception("Can't parse %r" % (s,))
h='18k'
print parse_size(h)


def log():
    pass


def decorator(func):
    def inner(*args,**kwargs):
        print 'this is a decorator'
        return func
    return decorator





class Init(object):
    def __init__(self,*args,**kwargs):
        self._args=args
        self.kwargs=kwargs

    @decorator
    def processArgs(self):
        for arg in self._args:
            if not arg:
                break
            print arg
        for kwarg in self.kwargs:
            print kwarg
            if  not isinstance(kwarg,str):
                break
            print ('args name:{} para:{}').format(kwarg,self.kwargs[kwarg])

    def Count(self,number):
        for i in xrange(number):
            yield  i

    def Counter(self,number):

        if not isinstance(number,int):
            raise ValueError('counter  must be a number')
        self.counter=self.Count(number)
        for count in self.counter:
            if count<10:
                print 'count {}'.format(count)
            else:
                break
        print 'count over'

    def check_ele_null(self,*args):
        pass



i=Init('hello','test',kangkai='test')
i.processArgs()
i.Counter(100)



def test(func):
    def inner():
        print 'innner'
        ret=func()
        return ret

    return inner
@test
def hello():
    print 'hello'
    return 0

hello()
enumerate(zip([1,2,3],['h','l','s']))

s=filter(lambda x:x+1,range(100))
print s
print type(s)
items = [1, 2, 3, 4, 5]
squared = list(map(lambda x: x**2, items))
print squared

class Like(object):
    def __new__(cls, *args, **kwargs):
        pass

    def __init__(self,obj):
        self.obj=obj
        





