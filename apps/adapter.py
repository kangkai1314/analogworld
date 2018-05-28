#--*-- coding:utf-8 --*--

class T():
    def request(self):
        print 'normal request'
        return 'normal'


class A():
    def special_request(self):
        print 'special request'
        return 'special'



class Adapter(T):
    def __init__(self):
        self.adapater=A()

    def request(self):
        self.adapater.special_request()


class BaseAdapter(object):
    def __init__(self,obj,adapted_methods):
        self.obj=obj
        self.__dict__.update(adapted_methods)
        

    def __str__(self):
        return str(self.obj)


if __name__ == '__main__':
    t=Adapter()
    t.request()
    objects = []
    t1=T()
    objects.append(t1)
    t2=A()
    objects.append(BaseAdapter(t2,dict(request=t2.special_request)))

    for i in objects:
        print i
        i.request()
        #print i.request()
