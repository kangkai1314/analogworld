#--*-- coding:utf-8 --*--
import unittest
from apps.dbcmp import *

from utils.db import *

class TestCompare(unittest.TestCase):
    def setUp(self):
        self.conn='atptest/atptest@oel1247'
        self.dbobj=None
        self.dbobj = DBObj(self.conn)
        self.table='dr_ggprs_1_20141222_7720'

    def test_db(self):
        self.dbobj=DBObj(self.conn)
        sql='''select count(*) from More_dr_ggprs_20141222_7720;'''
        lst,msg=self.dbobj.execute(sql)
        print msg

    def test_totalfeecount(self):
        t=TotalFeeCount('dr_ggprs_1_20141222_7720',0)
        gen_sql=t.generate()
        print gen_sql
        #self.assertEqual('sql',gen_sql)
        l,s=t.run(self.dbobj)
        print l,s

    def test_oldmorefeecount(self):
        t=OldMoreFeeCount(self.table)
        t.run(self.dbobj)

    def tearDown(self):
        self.dbobj=None