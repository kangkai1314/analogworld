#--*-- coding:utf-8 --*--
from collections import namedtuple

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

def main():
    c=Call()
    print type(Call)
    print type(c)
    print c


if __name__ == '__main__':
    main()





