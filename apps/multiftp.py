#--*-- coding:utf-8 --*--
import os
import  threading
import subprocess
from itertools import groupby
from threading import current_thread

thread_num=10

class MultiFtp(object):

    def __init__(self,src_path,dest_path,user,passwd,host,filelst=None):
        self.src_path=src_path
        self.dest_path=dest_path
        self.user=user
        self.host=host
        self.passwd=passwd
        self.filelst=filelst

    def checkdir(self):
        return os.path.exists(self.src_path)

    def checkftp(self,times=3):
        count=0
        cmd='''ftp -i -n {host} {user} {passwd}'''.format(host=self.host,user=self.user,
                                                          passwd=self.passwd)
        ret=os.system(cmd)
        if ret:
            return True
        else:
            count+=1
            self.checkftp()

    def checkfile(self):
        normalfilelst=[]
        errfilelst=[]

        if self.filelst is None:
            self.filelst=os.listdir(self.src_path)

        for f in self.filelst:
            print f

            if f in os.listdir(self.src_path):
                if os.path.isfile(os.path.join(self.src_path,f)):
                    normalfilelst.append(f)
                else:
                    errfilelst.append(f)
            else:
                errfilelst.append(f)

        if errfilelst!=os.listdir(self.src_path):
            ret=True
        else:
            ret=False
        return ret,normalfilelst,errfilelst


    def singftp(self,filelst):
        print current_thread().name
        for file in filelst:
            print 'processfile:%s'%(file)
            if file:
                self.dosingftp(file)

    def dosingftp1(self,filename):
        print filename




    def dosingftp(self,filename):

        cmd = '''ftp -i -n %s >/dev/null <<EOF
                                              user %s %s
                                          ''' % (self.host, self.user, self.passwd)
        cmd += '''lcd %s
                                              ''' % (self.src_path)
        cmd += '''cd  %s
                                              ''' % (self.dest_path)
        cmd += '''bin              
                                              '''
        cmd += '''%s %s
                                              ''' % ('mput', filename)
        cmd += '''EOF
                                              '''

        cmdexec = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        out = cmdexec.communicate()
        print out
        return out



    def run(self):
        threadlst=[]
        ret, normalfilelst, errfilelst=self.checkfile()
        if ret:
            length=len(normalfilelst)
            print length
            count=length/10
            print count
            count+=1
            group=self.groupby(normalfilelst,count)
            print group

            for g in group:
                print g
                t=threading.Thread(target=self.singftp,args=(g,))
                threadlst.append(t)

            for t in threadlst:
                t.start()

            for t in threadlst:
                t.join()

    def groupby(self,filelst,num):
        print filelst
        print num
        for i in xrange(0,len(filelst),num):
            yield filelst[i:i+num]


















