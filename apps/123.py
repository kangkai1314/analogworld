import sys
from collections import OrderedDict
def multipliters():
    return [lambda x:i * x for i in range(4)]
print multipliters()

print([m(2) for m in multipliters()])
print 1 or 2
print 1 and 2

v1 = [i % 2 for i in range(10)]
v2 = (i % 2 for i in range(10))
print(v1,v2)
v={3:5}
alist = ['a','b','c','d','e','f']
blist = ['x','y','z','d','e','f']
print list(zip(alist,blist))

list=[1,2,3,4,5]
for i in range(len(list)-1,-1,-1):
    print list[i]

print type(lambda x,y:x+y)


def echo():
    while True:
        received=yield
        print 'received from :{}'.format(received)

e=echo()
next(e)
e.send('hello')

class Ecc():
    def __init__(self):
        pass
    def echo(self):
        for i in xrange(100):
            num=yield





print sys.exc_info()

a=dict(
    hello='1',
    test=2
)

b=OrderedDict(
    hello=1,
    test=2
)


class Query():
    def __init__(self,name):
        self.name=name

    def __enter__(self):
        print 'begin'
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print 'Error'
        else:
            print 'End'

    def query(self):
        print 'Query name {}'.format(self.name)




with Query('name'):
    Query.query()

















