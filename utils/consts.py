#--*-- coding:utf-8--*--

World_Length=100
NameList=[u'唐秋月',u'王立倩',u'金依依',u'刘利荣',u'刘娜']
keyList=['love','career','study','daily']
items=[1,2,3,4,5]
IP_PORT=('127.0.0.1',3333)
s=[]
for i in items:
    s.append(i)

s1=list(map(lambda x :x**2 ,items))
print s1

def m(x):
    return (x*x)

def a(x):
    return x+x
funcs=[m,a]
for i in range(5):
    va=map(lambda x:x(i),funcs)
    print va