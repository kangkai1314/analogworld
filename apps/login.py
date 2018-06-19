#--*-- coding:utf-8 --*--
import os,sys
import requests

from module.user import User
from utils.check import *
from module.message import *

UserDict={'kangkai':123456}
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
    def __init__(self,name,passwd):
        self.msg=None
        self.username=name
        self.passwd=passwd
        self.login_time=None

    def __call__(self, ):
         return self.run()



    def check(self,user):
        if isinstance(user,User):
            return 'check success'
        else:
            raise TypeError('check func not get correct param')

    def run(self):
        if self.username in UserDict:
            if self.passwd==UserDict[str(self.username)]:
                print 'login success'
                return True
            else:
                raise TypeError('user passwd is wrong')
        else:
            raise TypeError('username is wrong')




class Regist(Action):
    action_id = 3
    def __init__(self,name,passwd):
        self.username=name
        self.passwd=passwd
        self.regist_time=None

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

    def get_regist_time(self):
        return self.regist_time

class Logout(Action):
    action_id = 5

    def __init__(self):
        self.user=None
        self.logout_time=None


    def run(self,*args,**kwargs):
        if self.user:
            pass

    def get_logout_time(self):
        return self.logout_time

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




if __name__ == '__main__':
    main()