#--*-- coding:utf-8 --*--
from collections import Iterable,Iterator

a={'1':2,'2':3}
class Iter:
    def __init__(self):
        pass
    def __iter__(self):
        return self

    def next(self):
        yield [i for i in range(2)]


def func():
    for k in a:
        yield a[k]

if isinstance(a,Iterator):
    print 'is  a Iterator'

if isinstance(a,Iterable):
    print 'is a Iterable'


f=func()
print next(f)
print f.next()
try:
    for i in range(0):
        print f.next()
except StopIteration('this is test'):
    print 'stopiert'

i=Iter()
print i
for j in range(5):
    print i.next()
    print i.next().next()
    i.next()
    i.next()


class newLine(object):
    def __new__(cls, *args, **kwargs):
        print cls
        return cls.__init__(object.)


n=newLine()

