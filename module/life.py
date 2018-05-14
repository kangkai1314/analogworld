#--*-- coding:utf-8 --*--
from functools import wraps
from module import user


def check_user( func):
    @wraps(func)
    def wrapper(self,*args, **kwargs):
        if isinstance(self.current_user, user.User):
            ret = func(*args, **kwargs)
        else:
            raise TypeError('not a user obj')

    return wrapper
class Life(object):
    def __init__(self,world_time):
        self.life_time=world_time
        self.current_user=None

    @check_user
    def initLife(self):
        print 'welcome to your life   \n'
        print 'there your are the owner ----{}'.format(self.current_user.name)
        if self.life_time>0 and self.life_time<10:
            return self.build_life()
        elif self.life_time>=10 and self.life_time<25:
            return self.build_life()
        elif self.life_time>25 and self.life_time<50:
            return self.build_life()
        else:
            return self.build_life()

    def build_life(self):
        pass

    def start(self,*agrs,**kwargs):
        raise NotImplementedError




class youth(Life):
    type=1

    def __init__(self):
        super(Life,self).__init__()
        self.life_length=5


    def start(self):
        print 'your youth is start'



class Story():
    def __init__(self):
        self.key=None


    def BuildStory(self):
        pass


class YouthStory(Story):
    def __init__(self):
        super(Story,self).__init__()














