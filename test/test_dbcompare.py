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
        t.run({},'totalfeecount')


    def test_oldmorefeecount(self):
        t=OldMoreFeeCount(self.table)
        t.run({},'oldmorefeecount')


    def test_compare(self):
        table_name='dr_sms_$specday'
        flag=1
        com=Compare(table_name,flag)
        day=com.get_curr_days
        print day
        busi=com.get_busi_type
        print busi
        print com.checkTable()
        self.assertEqual(True,com.checkTable())
        lst=com.build_table()
        print lst


    def test_dbcompare(self):
        table_name='dr_ggprs_$region_code_$specday'
        flag=1
        dbcom=DbCompare(table_name,flag)
        dbcom.build_table()
        #dbcom.build_compare_content(t)
        dbcom.run()





    def tearDown(self):
        self.dbobj=None