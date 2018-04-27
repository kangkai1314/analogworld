#--*-- coding:utf-8 --*--

'''
friend :
user can add friend ,remove friend

'''
class Relation():
    type=1

    def __init__(self,user):
        self.user=user

class Strangers(Relation):
    type=2

    def __init__(self,user):
        Relation.__init__(user)
        self.status=1

class Friend(Relation):
    def __init__(self,user):
        Relation.__init__(user)

    def get_friends(self):
        if self.user:
            friend_dict={}
            return friend_dict

    def add_friend(self,request):
        if request:
            pass

    def remove_friend(self,friend_id):
        friendlst=self.get_friends()
        if friend_id:
            friendlst.remove(friend_id)



class Closefriend(Friend):
    def __init__(self,user):
        friend.__init__(user)
        self.type=3

    def add(self,user):
        return closefriend

    def remove_friend(self,friend_id):
        pass



class Blackfriend(Relation):
    def __init__(self,user):
        self.user=user













