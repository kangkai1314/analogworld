#--*-- coding:utf-8 --*--
import Queue
import threading
from collections  import defaultdict

class EventEgine():
    def __init__(self):
        self.queue=Queue.Queue()
        self.flag=None
        self.thread=threading.Thread()
        self.handlers=defaultdict()


    def run(self):
        while self.flag:
            try:
                event=self.queue.get(block=False,timeout=1)
                self.process(event)
            except Exception as e:
                print str(e)

    def process(self,event):
        if event.type_ in self.handlers:


