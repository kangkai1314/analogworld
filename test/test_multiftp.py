#--*-- coding:utf-8 --*--
import unittest
from multiftp import MultiFtp

class TestMultiFtp(unittest.TestCase):
    def setUp(self):
        self.src='/data01/usergrp/hujf/kangkai/code'
        self.dest='/data01/usergrp/pengjh3/kangkai/destdata'
        self.user='pengjh3'
        self.passwd='pengjh3'
        self.host='10.10.12.171'

    def test_multiftp(self):
        mf=MultiFtp(self.src,self.dest,self.user,self.passwd,self.host)
        ret=mf.checkdir()
        self.assertEqual(True,ret,'source file is existed')
        r,n,e=mf.checkfile()
        #self.assertEqual(True,r,'all file is existed')
        #r=mf.dosingftp('a.txt')
        #print r


        mf.run()

    def test_ftp(self):
        mf = MultiFtp(self.src, self.dest, self.user, self.passwd, self.host)
        r, n, e = mf.checkfile()
        #r=mf.singftp(n)
        #print r



    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
