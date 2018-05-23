#--*-- coding:utf-8 --*--
from itertools import groupby

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


    def __iter__(self):
        return self

    def next(self):
        if self.threadlst:
            for i in self.threadlst:
                yield i
        else:
            raise TypeError('self.threadlst is not a list')











def main():
    l=[1,2,3,4,5,6]
    for i in xrange(0,len(l),1):
        print l[i:i+1]



 


if __name__ == '__main__':
    main()
