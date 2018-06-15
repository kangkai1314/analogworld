#--*-- coding:utf-8 --*--
from collections import namedtuple
from functools import reduce
from operator import mul
from operator import itemgetter

Card=namedtuple('Card',['rank','suit'])

class Poker:

        ranks=[i for i in range(2,11)]+list('JQKA')
        suits='spades diamond clubs hearts'.split()

        def __init__(self):
            self._cards=[Card(rank,suit) for rank in self.ranks for suit in self.suits]

        def __len__(self):
            return len(self._cards)

        def __getitem__(self, position):
            return self._cards[position]

        def __str__(self):
            return 'str'


p=Poker()
print p._cards
print len(p)

class Vector():
    def __init__(self,x,y):
        self.x=x
        self.y=y

    def __repr__(self):
        return 'Vector({},{})'.format(self.x,self.y)

    def __add__(self, other):
        return Vector(self.x+other.x,self.y+other.y)

a=lambda x:x >10
print a(100)

h= ((a,b) for a in range(10) for b in range(10))
next(h)
next(h)
print zip([a for a in range(10)],[b for b in range(10) ])

friuts=['hello','world','l','m']
l=sorted(friuts,key=len)
print friuts
print l
def reverse(word):
    return word[::-1]
r=reverse('hello')
print r
l2=sorted(friuts,key=reverse)
print l2
for i in map(reverse,friuts):
    print i

print list(map(reverse,friuts))

print [reverse(i) for i in friuts ]
print reverse(filter(lambda x:len(reverse(x))>2, friuts))
print [reverse(i) for i in friuts if len(reverse(i))>2]
print reduce(mul,range(1,10))
friuts1=['hello','world','l','m',False]

print all(friuts1)
print any(friuts1)

import random
class BingoCase(object):
    def __init__(self,items):
        self._items=list(items)
        random.shuffle(self._items)

    def pick(self):
        try:
            return self._items.pop()
        except IndexError:
            raise LookupError('can not find ele')

    def __call__(self, *args, **kwargs):
        return self.pick()


b=BingoCase(friuts)
print callable(b)
'''input ele name  content '''
def tag(name, cls=None,*content, **attrs):
    if cls is not None:
        attrs['class'] = cls
    if attrs:
        attr_str = ''.join(' %s="%s"' % (attr, value)
        for attr, value in sorted(attrs.items()))
    else:
        attr_str = ''
    if content:
        return '\n'.join('<%s%s>%s</%s>' %(name, attr_str, c, name) for c in content)
    else:
        return '<%s%s />' % (name, attr_str)

print tag('p')
print tag('p',None,'hello')
print tag('p',None,'hello',id=33)

def clip(text,max_length=100):
    end=None
    if len(text)>max_length:
        space=text.rfind('',0,max_length)
        if space>-1:
            end=space
        else:
            space=text.rfind('',max_length)
            if space>-1:
                end=space
    if end is None:
        end=len(text)

    return text[:end].strip('')

ttt='hello hello1 hello2 '
print clip(ttt,100)
pythonele=[
    (1,'list',['mutable']),
    (2,'tuple',['unmutable']),
    (3,'dict',['mutable'])
]
for p in sorted(pythonele,key=itemgetter(1,0)):
    print p

from functools import partial

func1=partial(mul,7)
print func1(10)

from abc import ABCMeta


class Lineitem(object):
    def __init__(self,product,quantity,price):
        self.product=product
        self.quantity=quantity
        self.price=price

    def total(self):
        return self.quantity*self.price





class Order(object):
    def __init__(self,customer,cart,promotion=None):
        self.total=0
        self.customer=customer
        self.cart=list(cart)
        self.promotion=promotion

    def total(self):
        if  not hasattr(self,'_total'):
            self._total=sum(item.total for item in self.cart)
        return self._total

    def due(self):
        if self.promotion is None:
            discount=0
        else:
            discount=self.promotion.disctount(self)
        return self.total-discount



class Promotions:
    def discounts(self,order):
        raise NotImplementedError



class Apromotions(Promotions):
    def discounts(self,order):
        return order.total*0.05 if order.customer.ider>100 else 0


class BPromotions(Promotions):
    def discounts(self,order):
        discount=0
        for item in order.cart:
            if item.quantity>20:
                discount+=order.total*0.1

        return discount

class CPromotions(Promotions):
    def discounts(self,order):
        distinct_items={item.product for item in order}
        if len(distinct_items)>=10:
            return order.total*0.07
        return 0


from functools import wraps
registary=[]


def register(func):
    print 'running register {}'.format(func)

    registary.append(func)
    return func
@register
def f1():
    print 'f1'
print 'running func1'
f1()

import time

def clock(func):
    def deco(*args):
        start=time.clock()
        result=func(*args)
        end=time.clock()
        print end
        return result
    return deco
@clock
def add(x,y):
    sum=0
    for i in range(100):
        sum+=(x+y)

    return sum

l=add(10,100)
print l

class Bus:
    def __init__(self,passageners=None):
        if passageners is None:
            self.passageners=[]
        else:
            self.passageners=list(passageners)

    def pick(self,name):
        self.passageners.append(name)

    def drop(self,name):
        self.passageners.remove(name)

import copy

b=Bus(['bob','Alice'])
b1=copy.copy(b)
b2=copy.deepcopy(b)
print id(b1),id(b2),id(b)
b1.drop('bob')
print b2.passageners
print b.passageners

import weakref
s1={1,2,3}
s=s1
def bye():
    print 'goodbye'

del s


















