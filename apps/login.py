#--*-- coding:utf-8 --*--
import os,sys
import requests

from module.user import User
from utils.check import *
from module.message import *

UserDict={}
Music_Path=u'D:\CloudMusic'

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
    action_id = 5

    def __init__(self):
        self.user=None


    def run(self,*args,**kwargs):
        if self.user:
            pass



class PlayMusic(Action):
    action_id = 4

    def __init__(self,song_id):
        self.song_id=song_id
        self.session=requests.Session()


    def get_song_src(self):
        path=self.get_song_location()
        if path:
            print path
            filelst=os.listdir(path)
            return [Music_Path+'\\'+i for i in filelst if i.endswith('mp3')]

    def get_song_location(self):
        return Music_Path


    def run(self,*args,**kwargs):
        music=self.get_song_src()
        os.system(music)







def main():
    p=PlayMusic(1)
    h=p.get_song_src()
    print h
    print h[0].encode('utf-8')

if __name__ == '__main__':
    main()