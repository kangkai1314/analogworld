#--*-- coding:utf-8 --*--

from functools import wraps

def decorator(func):
    @wraps(func)
    def inner():
        print 'before'
        func()
        print 'after'
    return inner

@decorator
def play():
    print 'play'


play()


from functools import wraps
def decorator_name(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not can_run:
            return "Function will not run"
        return f(*args, **kwargs)
    return decorated
@decorator_name
def func():
    return("Function is running")
can_run = True
print(func())
# Output: Function is running
can_run = False
print(func())
# Output: Function will not run



class App(object):
    def __new__(cls, iterable, fileds):
        instance=object.__new__(cls)
        if isinstance(instance,cls):
            print 'is a instance'
        else:
            raise TypeError('not a instacnce')
        for k,v in enumerate(iterable):
            if hasattr(cls,v):
                print 'has attr'
            else:
                print v
                setattr(instance,v,fileds[k])
        return instance

    def __init__(self,iterable,fields):
        self.iterable=iterable
        self.fileds=fields

    def __repr__(self):
        return 'test'

    def run(self):
        for v in self.iterable:
            if hasattr(self,v):
                print 'has attr'


a=App(['name','age'],['kangkai',12])
print a.__repr__()
a.run()
print a.name
print a.age


