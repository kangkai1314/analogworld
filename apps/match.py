#--*-- coding:utf-8 --*--

'''
User click  button start to find a user to chat
action:
user move in to WaitQueue,

'''
import Queue
class Match():
    def __init__(self):
        self.curruser=None


    def get_wait_queue(self):
        return Queue.Queue()

    def run(self):
        self.queue=self.get_wait_queue()
        if self.queue:
            self.curruser.status=False#User can not enter
            for user in self.queue:




    def __exit__(self, exc_type, exc_val, exc_tb):
        self.curruser.status=True

