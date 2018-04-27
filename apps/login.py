#--*-- coding:utf-8 --*--

from module.user import User
from utils.check import *
UserDict={}

class Action(object):
    action_id=1

    def run(self,*args,**kwargs):
        raise NotImplementedError


    def check(self,*args,**kwargs):
        raise NotImplementedError



class Login(Action):
    '''
    login action:
    verify the account is correct
    verify the passwd is correvt

    '''
    action_id = 2
    def __init__(self):
        self.msg=None


    def check(self,user):
        if isinstance(user,User):
            return 'check success'
        else:
            raise TypeError('check func not get correct param')

    def run(self,account,passwd):
        c=ERROR()
        print c



class Regist(Action):
    action_id = 3
    def __init__(self):
        pass

    def check(self,*args,**kwargs):
        pass

    def run(self,*args,**kwargs):
        pass


class PlayMusic(Action):
    action_id = 4

    def __init__(self):
        pass

    def check(self):





def main():
    path='/www.baidu.com'
    print is_absolute(path)


if __name__ == '__main__':
    main()