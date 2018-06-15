#--*-- coding:utf-8 --*--
class A(object):
    def __init__(self,a):
        self.a=a


def main():
    a=A('helo')
    a.name='hello'
    print a.name

if __name__ == '__main__':
    main()