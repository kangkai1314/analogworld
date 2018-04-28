#--*-- coding:utf-8 --*--

from module.user import User
from utils.check import *
from module.message import *
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
    def __init__(self,user):
        self.msg=None
        self.user=user


    def check(self,user):
        if isinstance(user,User):
            return 'check success'
        else:
            raise TypeError('check func not get correct param')

    def run(self,account,passwd):
        if account is self.user._account and passwd is self.user._getpasswd():
            return  'login success'
        else:
            print 'username or passwd is wrong ,please check'
            return 'login failed'





class Regist(Action):
    action_id = 3
    def __init__(self,user):
        self.user=user

    def check(self,*args,**kwargs):
        if self.user:
            if self.user in UserDict:
                return CheckMessage('debug','User is already be registed')
            else:
                self.run()


    def run(self,*args,**kwargs):
        if self.user:
            pass

    def write_user_info(self):
        username=self.user._account
        passwd=self.user._getpasswd()



class Logout(Action):
    





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