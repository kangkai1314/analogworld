#--*-- coding:utf-8 --*--

'''
db比对算法

'''
import threading
import datetime
from apps.compare import *

CmpDict={'1':'TotalFeeCount',
         '2':'ConsistentFeeCount',
         '3':'OldMoreFeeCount',
         '4':'NewMoreFeeCount',
         '5':'UnconsistentCount',
         '6':'UnconsistentFee'}
class Sql(object):
    type=0#话单条数

    def __init__(self,table_name):
        self.table_name=table_name
        self.busi=self.get_busi()

    def get_busi(self):
        return self.table_name.split('_')[1]

    def do_preconditon(self):
        raise NotImplementedError

    def generate(self,*args,**kwargs):
        raise NotImplementedError

    def run(self,*args,**kwargs):
        raise NotImplementedError

    def check(self):
        raise NotImplementedError

    def get_dbuser(self,flag=None):
        if flag is None:
            dbusers=''
            return
        dbuser=''
        return dbuser

class TotalFeeCount(Sql):
    type=1#话单和费用总数

    def __init__(self,table_name,flag):
        Sql.__init__(table_name)
        self.flag=flag


    def generate(self):
        dbuser = self.get_dbuser(self.flag)
        if self.busi is not 'upload':
            base_sql='''select count(*), sum(charge1+charge2+charge3+charge4) from {DB_USER}.{TABLE_NAME}'''.format(DB_USER=dbuser,TABLE_NAME=self.table_name)
            return self.flag,base_sql
        else:
            base_sql = '''select count(*) from {DB_USER}.{TABLE_NAME}'''.format(DB_USER=dbuser, TABLE_NAME=self.table_name)
            return self.flag, base_sql

    def run(self):
        sql=self.generate()
        if sql:
            ret=sql.execute()
            return self.flag,ret


class ConsistentFeeCount(Sql):
    type=2#一致的话单数和费用

    def __init__(self,table_name):
        Sql.__init__(table_name)

    def do_preconditon(self):
        sql='''create table as (select * from ud_old.{table}) minus (select * from ud_new.{table})'''

    def generate(self,*args,**kwargs):
        base_sql='''sel'''
        return base_sql

    def run(self):
        pass


class OldMoreFeeCount(Sql):
    type=3#旧有新无

    def __init__(self,table_name):

        Sql.__init__(table_name)
        #dr_gsm_201708

    def do_preconditon(self):
        tmp_table='More_Old_%s'%(self.table_name)
        old_table=''
        new_table=''
        create_table='''create table {tmp_tab}as select * from {old_table} minus select * from {new_table}'''.format(tmp_tab=tmp_table,old_table=old_table,new_table=new_table)
        create_table.excute()
        return tmp_table
    def generate(self):
        tmp=self.do_preconditon()
        if tmp:
            base_sql='''select count(*), sum(charge1+charge2+charge3+charge4) from {tmp}'''.format(tmp=tmp)
        return base_sql
    def run(self):
        sql=self.generate()
        if sql:
            ret=sql.execute()
            return ret


class NewMoreFeeCount(Sql):
    type=4

    def __init__(self,table_name):
        Sql.__init__(table_name)

    def do_preconditon(self):
        tmp_table = 'More_New_%s' % (self.table_name)
        old_table = ''
        new_table = ''
        create_table = '''create table {tmp_tab}as ((select * from {new_table}) minus (select * from {old_table}))'''.format(
            tmp_tab=tmp_table, old_table=old_table, new_table=new_table)

        msg=create_table.excute()
        if msg=='success':
            pass
        else:
            return False
        return tmp_table

    def generate(self):
        tmp=self.do_preconditon()
        if tmp:
            base_sql='''select count(*), sum(charge1+charge2+charge3+charge4) from {tmp}'''.format(tmp=tmp)
        return base_sql

    def run(self):
        sql=self.generate()
        if sql:
            ret=sql.execute()
            return ret

class UnconsistentCount(Sql):
    type=5#不一致的条数

    def __init__(self,table_name):
        Sql.__init__(table_name)

    def generate(self,*args,**kwargs):
        base_sql=''''''
        return base_sql

class UnconsistentFee(Sql):
    type=6#不一致的费用数

    def __init__(self,table_name,flag):
        Sql.__init__(table_name)
        self.flag=flag

    def generate(self,*args,**kwargs):
        base_sql=''''''
        return base_sql
    def run(self,sql):
        if sql:
            pass
        else:
            raise Exception('')



class Builder():
    def __init__(self,table_name):

        self.table_name=table_name

    def config_dbcompare(self):
        self.db_compare=DbCompare(self.table_name)
        self.db_compare.totalfeecount=TotalFeeCount(self.table_name)
        self.db_compare.consistentfeecount=ConsistentFeeCount(self.table_name)
        self.db_compare.oldmorefeecount=OldMoreFeeCount(self.table_name)
        self.db_compare.newmorefeecount=NewMoreFeeCount(self.table_name)
        self.db_compare.unconsistentcount=UnconsistentCount(self.table_name)
        self.db_compare.unconsistentfee=UnconsistentFee(self.table_name)

    def config_mdbcompare(self):
        self.mdb_compare=MdbCompare(self.table_name)
        self.mdb_compare.totalfeecount(self.table_name)

    def config_errcompare(self):
        self.errcompare=Compare(self.table_name)
        self.errcompare.totalfeecount=TotalFeeCount(self.table_name)


    def config_uploadcompare(self):
        self.upload_compare=Compare(self.table_name)
        self.upload_compare.totalfeecount=TotalFeeCount(self.table_name)



class ComparasionResult():

    def __init__(self,table_name):
        self.table_name=table_name
        self.result_dic={}

    def CaculatePassRate(self):

        rate=''

        return rate

    def ProcessToReport(self):
        report={}
        for key,items in self.result_dic:
            if key is 'TotalFeeCount':
                total_fee,total_count=items
            else:
                report[key]=items
        pass_rate=self.CaculatePassRate()
        report['pass_rate']=pass_rate


    def insert(self):
        if self.result_dic:
            sql='''insert into at_tc_dbcmp_result   '''






























