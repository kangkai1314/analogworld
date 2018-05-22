#--*-- coding:utf-8 --*--


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

    def __init__(self):
        super(BaseListener,self).__init__()
        self.threadlst=[]






def main():
    bl=BaseListener()
    bl()
    t=ThreadListener()
    t()
 


if __name__ == '__main__':
    main()
