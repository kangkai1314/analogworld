
def multipliters():
    return [lambda x:i * x for i in range(4)]
print multipliters()

print([m(2) for m in multipliters()])
print 1 or 2
print 1 and 2

v1 = [i % 2 for i in range(10)]
v2 = (i % 2 for i in range(10))
print(v1,v2)
v={3:5}
alist = ['a','b','c','d','e','f']
blist = ['x','y','z','d','e','f']
print list(zip(alist,blist))

list=[1,2,3,4,5]
for i in range(len(list)-1,-1,-1):
    print list[i]

print type(lambda x,y:x+y)




