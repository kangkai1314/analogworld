#--*-- coding:utf-8 --*--
import unittest
from regress.dbcmp import *
from regress.atdb import *


class TestCompare(unittest.TestCase):
    def setUp(self):
        self.conn='atptest/atptest@oel1247'
        self.dbobj=None
        self.dbobj = DBObj(self.conn)
        self.table='dr_ggprs_701_201805'

    def test_db(self):
        self.dbobj=DBObj(self.conn)
        sql='''select count(*) from More_dr_ggprs_20141222_7720;'''
        lst,msg=self.dbobj.execute(sql)
        print msg

    def test_totalfeecount(self):
        t=TotalFeeCount(self.table,0)
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
        com=Compare(table_name,flag,102)
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
        dbcom=DbCompare(table_name,flag,102)
        dbcom.build_table()
        #dbcom.build_compare_content(t)
        result=dbcom.run()
        dbcom.update_result(result)

    def test_report(self):
        r=ComparasionResult('dr_ggprs')
        r.report_init()
        print r.report
        print r.result_dic

    def test_mdbcompare(self):
        pass

    def test_uploadcompare(self):
        pass

    def tearDown(self):
        self.dbobj=None


if __name__ == '__main__':
    unittest.main()