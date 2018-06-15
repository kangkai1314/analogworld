#--*-- coding:utf-8 --*--
import abc
import re
class Interview():

    def __init__(self,name):
        self.no=1
        self.result=None
        self.interviewer=name

    def get(self):
        print 'how to introduce what you did?'
        print 'how to solve algorithms'
        print 'how to introduce your project'


    def put(self):
        pass


class Tombola():
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def load(self,iteable):
        '''实现基础的方法'''

    @abc.abstractmethod
    def pick(self):
        '''随机删除元素'''

    def loaded(self):
        return bool(self.inspect())

    def inspect(self):
        items=[]
        while True:
            try:
                items.append(self.pick())
            except LookupError:
                break
        self.load(items)
        return tuple(sorted(items))



class T(Tombola):
    def pick(self):
        return 12




def reverse(str):

    return str[::-1]






def main():
    str1='ab'
    print str1[::-1]
    print reverse(str1)
    str='abc aba abb aaa'
    print str.split('ab')
    num=str.count('ab')
    print num
    reg=re.compile("(?=ab)")
    length=len(reg.findall(str))
    print length
    re.match(r'ab')




if __name__ == '__main__':
    main()



