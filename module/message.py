#--*-- coding:utf-8 --*--
'''
message
'''
from functools import wraps
class BaseMessage(object):
    __slots__ = ('_type','content')#允许创建的属性
    def __init__(self,*args,**kwargs):
        self._type=None
        self.content=None

    def check(self):
        return isinstance(self._type,int)
    def get_msg(self):
        pass

    def get_msg_type(self):
        return self._type

    def set_msg(self,msg):
        # any(x)判断x对象是否为空对象，如果都为空、0、false，则返回false，如果不都为空、0、false，则返回true

        # all(x)如果all(x)参数x对象的所有元素不为0、''、False或者x为空对象，则返回True，否则返回False
        if any(msg):
            pass
        else:
            return any(msg)

    def handle_msg(self,msg):
        raise NotImplementedError



class Message(BaseMessage):
    __slots__ = ()
    def __init__(self):
        BaseMessage.__init__()
        self._type=0


def main():
    data=''
    if  not data:#not None == not False == not '' == not 0 == not [] == not {} == not ()  False
        print 'hello'

    while True:
        if not data:
            print 'loop'
            break


    str1='hello'
    it=iter(str1)
    for i in it:
        print i






if __name__ == '__main__':
    main()
