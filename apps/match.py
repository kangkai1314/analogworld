#--*-- coding:utf-8 --*--

'''
User click  button start to find a user to chat
action:
user move in to WaitQueue,

'''
import Queue
from module.user import *
class Match():
    def __init__(self,user):
        self.curruser=user
        self.matchuser=None

    def get_wait_queue(self):
        return Queue.Queue()

    def run(self):
        self.queue=self.get_wait_queue()
        if self.queue:
            self.curruser.status=False#User can not enter
            self.matchuser=self.queue.get()
            if self.matchuser:
                msg='match success'
                return [self.curruser,self.matchuser],msg

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.curruser.status=True

def main():
    user = User('kangkai', 'hello', 2)
    matchobj=Match(user)
    matchobj.run()

if __name__ == '__main__':
    main()

