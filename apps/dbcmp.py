#--*-- coding:utf-8 --*--
'''
db比对算法

'''
import threading
import datetime,time
import inspect
from regress.compare_test import *
from functools import wraps
from regress.atdb import DBObj
from threading import current_thread
from contextlib import contextmanager

CmpDict={'1':'TotalFeeCount',
         '2':'ConsistentFeeCount',
         '3':'OldMoreFeeCount',
         '4':'NewMoreFeeCount',
         '5':'UnconsistentCount',
         '6':'UnconsistentFee'}

KeyFileds={'ggprs':['USER_NUMBER']}


class Logger():
    def __init__(self):
        self.lock = threading.RLock()

    def prints(self, msg):
        self.lock.acquire()
        print msg
        self.lock.release()

def check_table_exist(func,table, dbobj):
    @wraps(func)
    def wrapper(dbobj, func, table, *args, **kwargs):
        sql = '''select count(*) from {table} '''.format(table=table)
        lst, msg = dbobj.execute(sql)
        if 'ORA-00942' in msg:
            return False
        elif msg is 'select finished':
            ret = func(*args, **kwargs)
            return ret
        else:
            return False
    return wrapper

def check_table(dbobj):
    def decorator(self,func):
        @wraps(func)
        def wrapper(*args,**kwargs):
            check_sql='''select count(*) from {}'''.format(self.table)
            lst, msg = dbobj.execute(check_sql)
            if 'ORA-00942' in msg:
                return False
            elif msg is 'select finished':
                ret = func(*args, **kwargs)
                return ret
            else:
                return False
        return wrapper
    return decorator

def check_table_1(func):
    @wraps(func)
    def wrapper(self,*args, **kwargs):
        check_sql = '''select count(*) from {}'''.format(self.table_name)
        lst, msg = self.dbObj.execute(check_sql)
        print msg
        print check_sql
        if 'ORA-00942' in msg:
            return False
        elif msg == 'select finished':
            ret = func(self,*args, **kwargs)
            return ret
        else:
            raise Exception('other oracle reason cause table is not existed')
    return wrapper

def time_record(func):
    @wraps(func)
    def wrapper(*args,**kwargs):
        start=time.clock()
        ret=func(*args,**kwargs)
        end=time.clock()
        print 'execute sql consume %s s'%(str(end-start))
        return ret
    return wrapper



class Sql(object):
    type=0#话单条数

    def __init__(self,table_name):
        self.table_name=table_name
        self.busi=self.get_busi()
        self.dbObj=None
        self.lock=threading.RLock()
        self.log=Logger()
        self.keyfileds=None

    def __str__(self):
        return CmpDict[str(self.type)]

    def get_current_class_name(self):
        return inspect.stack()

    def get_busi(self):
        return self.table_name.split('_')[1]

    def do_preconditon(self):
        raise NotImplementedError

    def generate(self,*args,**kwargs):
        raise NotImplementedError
    @time_record
    def run(self,*args,**kwargs):
        raise NotImplementedError

    def check(self):
        raise NotImplementedError

    def get_db(self,flag=None):
        conn_old = 'ud_old/ud_old@newjfbx'
        conn_new='ud_new/ud_new@newjfbx'
        conn_ori='at/at@newjfbx'
        if flag is None:
            return [(i.split('/')[0],DBObj(i)) for i in [conn_new,conn_old,conn_ori]]
        elif flag==0:
            return [conn_old.split('/')[0] ,DBObj(conn_old)]
        elif flag==1:
            return [conn_new.split('/')[0],DBObj(conn_new)]
        elif flag==2:
            return [conn_ori.split('/')[0],DBObj(conn_ori)]
        else:
            raise ValueError('not existed flag ')

    @contextmanager
    def Lock(self):
        print 'add lock'
        self.lock.acquire()
        yield
        print 'release lock'
        self.lock.release()

    def update_result(self,result,ret):
        #print 'add lock'
        #self.lock.acquire()
        with self.Lock():
            result.update(ret)
        #self.lock.release()
        #print 'release lock'

    def update_table(self,key,value):
        #self.lock.acquire()
        with self.Lock():
            update_sql = ''''''
            self.dbObj.execute(update_sql)

        #self.lock.release()

    def get_keyfileds(self):
        return KeyFileds[self.busi]



class TotalFeeCount(Sql):
    type=1#话单和费用总数

    def __init__(self,table_name,flag):
        Sql.__init__(self,table_name)
        self.flag=flag
        self.dbuser,self.dbObj=self.get_db(self.flag)



    def generate(self):
        if self.busi is not 'upload':
            base_sql='''select count(*), sum(charge1+charge2+charge3+charge4) from {DB_USER}.{TABLE_NAME}'''.format(DB_USER=self.dbuser,TABLE_NAME=self.table_name)
            return self.flag,base_sql
        else:
            base_sql = '''select count(*) from {DB_USER}.{TABLE_NAME}'''.format(DB_USER=self.dbuser, TABLE_NAME=self.table_name)
            return self.flag, base_sql


    @time_record
    def run(self,result,key):
        t=threading.current_thread()
        print t.name

        print '{thread} start execute totalfeecount'.format(thread=t.name)
        flag,sql=self.generate()
        print sql
        if sql:
            lst,msg=self.dbObj.execute(sql)
            #log.prints(lst)
            sql_result=self.process_result(lst)
            re={}
            re[key]={str(self.flag):sql_result}
            print re
            self.update_result(result,re)
            return self.flag,re
        else:
            print 'failed'


    def process_result(self,lst):
        print 'process sql execute result'
        print lst
        if isinstance(lst,list):
            return [i for i in lst[0]]
        else:
            raise TypeError('result not a list')

class ConsistentFeeCount(Sql):
    type=2#一致的话单数和费用

    def __init__(self,table_name):
        Sql.__init__(self,table_name)
        self.flag=0
        self.dbuser='atptest'

    def do_preconditon(self):
        dbinfo=self.get_db()
        if isinstance(dbinfo,list) and len(dbinfo)==3:
            old_user=dbinfo[0][0]
            new_user=dbinfo[1][0]
        else:
            raise Exception('get dbuser and dbobj failed')
        sql='''create  tbale as (select * from {old_user}.{table}) minus (select * from {new_user}.{table})'''.format(old_user=old_user,table=self.table_name,new_user=new_user)
        lst,msg=self.dbObj.execute(sql)
        if 'finished' not in msg:
            raise Exception('build tmp table failed')

    def generate(self,):
        #tmp_table=self.do_preconditon()
        tmp_table='dr_sms'
        if tmp_table:
            base_sql = '''select count(*), sum(charge1+charge2+charge3+charge4) from {DB_USER}.{TABLE_NAME}'''.format(
            DB_USER=self.dbuser, TABLE_NAME=self.table_name)
        return self.flag, base_sql

    def run(self,result,key):
        '''
                    if msg is 'select finshed':
                return lst
            else:
                raise Exception('run failed ')
        :param result:
        :param key:
        :return:
        '''
        sql=self.generate()
        if sql:
            #self.dbObj.execute(sql)
            ret = 500
            re = {}
            re[key] = ret
            print re
            self.update_result(result, re)



class OldMoreFeeCount(Sql):
    type=3#旧有新无

    def __init__(self,table_name):
        Sql.__init__(self,table_name)
        #dr_gsm_201708

    def do_preconditon(self):
        dbinfo = self.get_db()
        if isinstance(dbinfo, list) and len(dbinfo) == 3:
            old_user = dbinfo[0][0]
            new_user = dbinfo[1][0]
        else:
            raise Exception('get dbuser and dbobj failed')
        tmp_table='More_Old_%s'%(self.table_name)
        old_table='%s'%(self.table_name)
        new_table='%s'%(self.table_name)
        create_table='''create table {tmp_tab} as select * from {old_table} minus select * from {new_table}'''.format(tmp_tab=tmp_table,old_table=old_table,new_table=new_table)
        print create_table
        #create_table.excute()

        sql='''insert /*+ parallel (c,8)  */ into {tmp_table} c
            select a.*,11 from(select /*+ parallel (a,8)  */ USER_NUMBER,GGSN_ADDRESS,START_TIME,CHARGING_ID,REC_SEQ_NO,SERVICE_CODE
            from {olduser}.{old_table} a
            minus
            select /*+ parallel (b,8)  */ USER_NUMBER,GGSN_ADDRESS,START_TIME,CHARGING_ID,REC_SEQ_NO,SERVICE_CODE
            from {newuser}.{new_table} b) d,
            {olduser}.{old_table}  a
            where a.USER_NUMBER=d.USER_NUMBER and a.GGSN_ADDRESS = d.GGSN_ADDRESS and a.START_TIME = d.START_TIME and a.CHARGING_ID = d.CHARGING_ID and a.REC_SEQ_NO = d.REC_SEQ_NO and a.SERVICE_CODE = d.SERVICE_CODE';
   

        '''.format(tmp_table=tmp_table,olduser=old_user,newuser=new_user,old_table=self.table_name,new_table=self.table_name)
        print sql
        s,w=self.gen_select_condition()
        sql1='''insert into {} c select a.*,11 from (select {s_condition} from {old_table} a minus select {s_condition} from {new_table}) d where {where_condition})'''.format(tmp_table,s_condition=s[0].strip(','),old_table=self.table_name,new_table=self.table_name,where_condition=w[0])
        print sql1
        return tmp_table

    def gen_select_condition(self):
        keyfield=self.get_keyfileds()
        print keyfield
        h=[i+',' for i in keyfield]
        print h
        return [i+',' for i in keyfield],['a.{i}=d.{i}'.format(i=i) for i in keyfield]



    def generate(self):
        tmp=self.do_preconditon()
        if tmp:
            base_sql='''select count(*), sum(charge1+charge2+charge3+charge4) from {tmp}'''.format(tmp=tmp)
        return base_sql

    def run(self,result,key):

        sql=self.generate()
        print sql
        if sql:
            #ret=self.dbObj.execute(sql)
            ret = 500
            re = {}
            re[key] = ret
            print re
            self.update_result(result, re)

class NewMoreFeeCount(Sql):
    type=4

    def __init__(self,table_name):
        Sql.__init__(self,table_name)

    def do_preconditon(self):
        tmp_table = 'More_New_%s' % (self.table_name)
        old_table = ''
        new_table = ''
        create_table = '''create table {tmp_tab} as ((select * from {new_table}) minus (select * from {old_table}))'''.format(
            tmp_tab=tmp_table, old_table=old_table, new_table=new_table)

        msg='success'

        sql='''
          insert /*+ parallel (c,8)  */ into {table} c
            select a.*,12 from
            (select /*+ parallel (a,8)  */ USER_NUMBER,GGSN_ADDRESS,START_TIME,CHARGING_ID,REC_SEQ_NO,SERVICE_CODE
            from {old_table} a
            minus
            select /*+ parallel (b,8)  */ USER_NUMBER,GGSN_ADDRESS,START_TIME,CHARGING_ID,REC_SEQ_NO,SERVICE_CODE
            from {new_table} b) d,
            {old_table}  a
            where a.USER_NUMBER=d.USER_NUMBER and a.GGSN_ADDRESS = d.GGSN_ADDRESS and a.START_TIME = d.START_TIME and a.CHARGING_ID = d.CHARGING_ID and a.REC_SEQ_NO = d.REC_SEQ_NO and a.SERVICE_CODE = d.SERVICE_CODE';
        '''.format(table=tmp_table,old_table=self.table_name,new_table=self.table_name)
        print sql


        #msg=create_table.excute()
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

    def run(self,result,key):
        sql=self.generate()
        if sql:
            #ret=self.dbObj.execute()
            ret = 500
            re = {}
            re[key] = ret
            print re
            self.update_result(result, re)

class UnconsistentCount(Sql):
    type=5#不一致的条数

    def __init__(self,table_name):
        Sql.__init__(self,table_name)

    def generate(self,*args,**kwargs):
        base_sql=''''''
        return base_sql

    def run(self,result,key):
        ret = 500
        re = {}
        re[key] = ret
        print re
        self.update_result(result, re)

class UnconsistentFee(Sql):
    type=6#不一致的费用数

    def __init__(self,table_name,flag):
        Sql.__init__(self,table_name)
        self.flag=flag

    def generate(self,*args,**kwargs):
        base_sql=''''''
        return base_sql

    def run(self,result,key):
        time.sleep(5)
        ret = 500
        re = {}
        re[key] = ret
        print re
        self.update_result(result, re)
        if True:
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

