#--*-- coding:utf-8 --*--
from collections import namedtuple

class NamedTuple(tuple):

    def __new__(cls, iterable, fileds):
        if hasattr(cls,iterable):


    def __init__(self,iterable,fileds):
        self.iterable=iterable
        self.fileds=fileds


    def __repr__(self):
        return 'NamedTuple'

    def init(self):
        for key,i in enumerate(self.iterable):
            print key,i





