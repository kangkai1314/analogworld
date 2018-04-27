#--*-- coding:utf-8 --*--
from functools import wraps
class User(object):

    def __init__(self,name,passwd,id):
        self._account=name
        self._passwd=passwd
        self._userId=id

    def __repr__(self):
        return '{}'.format(str(self._userId)+self._account)

    def _getpasswd(self):
        if self._passwd:
            return self._passwd


    @property
    def userid(self):
        if self.userid:
            return self._userId

    @userid.setter
    def userid(self,id ):
        if isinstance(id,int):
            self._userId=id

    @userid.getter
    def userid(self):
        return self._userId

    def get_user_status(self):
        return self._user_status_code


    def del_user(self):
        self._user_status_code=1
        self._userId=None

class Profile(object):
    def __init__(self,user):
        self.user=user
        self.sex=None
        self.tel=None
        self.birth=None

    def __call__(self, *args, **kwargs):
        print 'request'

    def GetProfile(self,instance,**kwargs):
        for i in kwargs:
            key=i
            if hasattr(instance,key):
                return kwargs[key]

    def SetProfile(self,instance,**kwargs):
        for i in kwargs:
            if hasattr(instance,i):
                instance.i=kwargs[i]
            else:
                raise AttributeError('class Profile not has the attr ')


def main():
    account='kangkai'
    passwd='12345'
    userid=1
    user=User(account,passwd,userid)
    print user
    p=Profile(user)
    print p.user._userId
    p.GetProfile(p,sex=0)
    p.SetProfile(p,sex=0,tel=18086630180,hello='text')




if __name__ == '__main__':
    main()

