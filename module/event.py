#--*-- coding:utf-8 --*--
import Queue
import threading
from collections  import defaultdict


class Observer():
    def __init__(self,name,subject):
        self.name=name
        self.subject=subject

    def update(self):
        pass



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
            pass

class Event(object):
    type='base'

    def __init__(self):
        pass

    def run(self,*args,**kwargs):
        raise NotImplementedError


class UiEvent(Event,Observer):
    type='ui'

    def __init__(self,name):
        super(Event,self).__init__()
        self.name=name


    def run(self):
        print '进入{}界面'.format(self.name)


    def update(self):
        self.run()











