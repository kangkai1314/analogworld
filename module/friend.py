#--*-- coding:utf-8 --*--

'''
friend :
user can add friend ,remove friend

'''
import time,datetime

class Relation():
    type=1

    def __init__(self,user):
        self.user=user

    def build_rela(self,*args,**kwargs):
        raise NotImplementedError

    def disslove_rela(self,*args,**kwargs):
        raise NotImplementedError

class Strangers(Relation):
    type=2

    def __init__(self,user):
        Relation.__init__(user)
        self.status=1

class Friend(Relation):
    def __init__(self,user):
        Relation.__init__(user)
        self.create_time=None
        self.friend_dict={}

    def get_friends(self):
        if self.user:
            friend_dict={}
            return friend_dict

    def add_friend(self,request):
        if request:
            c_time=datetime.datetime.now().strftime('%Y%M%D%h%m%s')
            self.create_time=c_time

    def remove_friend(self,friend_id):
        friendlst=self.get_friends()
        if friend_id:
            friendlst.remove(friend_id)

    def count_days_of_friend(self):
        if self.user:
            for fri in self.friend_dict:
                if fri:
                    now=datetime.datetime.now().strftime('%Y%M%S%h%m%s')
                    days=self.create_time-now
                    return days



class Closefriend(Friend):
    def __init__(self,user):
        Friend.__init__(user)
        self.type=3

    def add(self,user):
        return user

    def remove_friend(self,friend_id):
        pass



class Blackfriend(Relation):
    def __init__(self,user):
        Relation.__init__(user)


    def get_black_lists(self):
        return self.balck_list

    def build_rela(self,*args,**kwargs):
        if self.user:
            pass

    def disslove_rela(self,*args,**kwargs):
        if self.user:
            if self.get_black_lists():
                if self.user in self.black_list:
                    self.black_list.remove(self.user)
                    msg='disslove black freind success'
                    return msg















