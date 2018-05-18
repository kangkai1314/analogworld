#--*-- coding:utf-8 --*--
CmpDict={}

import dbcmp
import datetime
import threading
from collections import namedtuple,OrderedDict
Env_Tag=namedtuple('Env',['old','new'])

from contextlib import contextmanager

class ComparasionResult():

    def __init__(self,table_name):
        self.result_dic=None
        self.report={}
        self.busi=table_name.split('_')[1]

    def report_init(self):
        self.report=OrderedDict(
            busi=self.busi,
            old_count=0,
            new_count=0,
            consis_count=0,
            old_more_count=0,
            new_more_count=0,
            unconsis_count=0,
            old_fee= 0,
            new_fee= 0,
            consis_fee=0,
            old_more_fee= 0,
            new_more_fee= 0,
            unconsis_fee= 0,
            xdr_rate=0,
            fee_rate=0
        )



    def CaculatePassRate(self):
        print self.report['consis_count'],self.report['new_count']
        if self.report:

            if 'xdr_rate' in self.report:
                if  not any([self.report['consis_count'],self.report['new_count']]):
                    return
                self.report['xdr_rate']=self.report['consis_count']/self.report['new_count']
            elif 'fee_rate' in self.report:
                self.report['fee_rate']=self.report['consis_fee']/self.report['new_fee']
        return True

    def ProcessToReport(self):
        self.report_init()
        for key in self.result_dic:
            print key
            print self.result_dic[key]

            if key is 'TotalFeeCount':
                if '1' in self.result_dic[key]:
                    self.report['new_count'] = self.result_dic[key]['1'][0]
                    self.report['new_fee'] = self.result_dic[key]['1'][1]
                elif '0' in self.result_dic[key]:

                    self.report['old_count']=self.result_dic[key]['0'][0]
                    self.report['old_fee']=self.result_dic[key]['0'][1]

            elif key is 'OldMoreFeeCount':
                self.report['old_more_count'] = self.result_dic[key]
                self.report['old_more_fee'] = self.result_dic[key]

            elif key is 'UnconsistentFee':
                self.report['unconsis_fee']=self.result_dic[key]

            elif key is 'NewMoreFeeCount':
                self.report['new_more_count']=self.result_dic[key]
                self.report['new_more_fee']=self.result_dic[key]

            elif key is 'UnconsistentCount':
                self.report['unconsis_count']=self.result_dic[key]
            elif key is 'ConsistentFeeCount':
                self.report['consis_count'] = self.result_dic[key]
                self.report['consis_fee']=self.result_dic[key]
        self.CaculatePassRate()
        print self.report


    def insert(self):
        if self.report:
            insert_sql='''insert into table {table_name}({columns}) VALUES ({values})'''.format()


class ResultClean():
    def __init__(self,table):
        self.table=table

    def __enter__(self):
        if self.table:
            exist_sql='''select count(*) from {}'''.format(self.table)
            print exist_sql

            delete_sql='''delete from {table} where task_id={task}'''.format(table=self.table,task=102)
            print delete_sql

        else:
            raise Exception('Table is not existed')

    def __exit__(self, exc_type, exc_val, exc_tb):
        insert_sql='''insert into table {table} VALUES ({values})'''.format(table=self.table,values=None)
        print insert_sql





class Compare(ComparasionResult):
    def __init__(self,table_name,flag,task_id):
        ComparasionResult.__init__(self,table_name)
        self.table_name=table_name#table_name:dr_ggprs_770_20170809
        self.builder=None
        self.flag=flag
        self.threadlst=[]
        self.task_id=task_id
        self.e= Env_Tag(0,1)




    def init_result_table(self):
        insert_sql=''''''
        self.dbobj.execute(insert_sql)

    def checkTable(self):
        print self.table_name
        if any([self.table_name]):
            return True

    def construct_compare(self,*args,**kwargs):
        raise NotImplementedError

    def build_table(self):

        '''
        @:parameter self.table de_gsm_$region_code_$
        :return: tablenamelst like [dr_gsm_770_201804]
        '''
        try:
            busi=self.get_busi_type
            day=self.get_curr_days
        except Exception:
            raise  Exception('build table failed cause by day or busi')

        if any([busi,day]):
            table_splitor='_'
            table_str='dr_'+busi
            print table_str
            if busi=='gsm' or busi=='ggprs':
                all_region_codes=['770','771']
                return [table_str+table_splitor+i+table_splitor+day for i in all_region_codes]
            else:
                return [table_str+table_splitor+day]
        else:
            return []

    @property
    def get_busi_type(self):
        '''
        @:parameter:self.table #dr_$busi_$regiion_$specday
        :return: $busi
        '''
        if self.table_name:
            return self.table_name.split('_')[1]
        else:
            return False

    @property
    def get_curr_days(self):
        now_day=datetime.datetime.now().strftime('%Y%m')
        return now_day

    @contextmanager
    def _clean(self):

        yield
        if self.table_name:
            sql='''delete from {TABLE_NAME} where task_id={TASK_ID} '''.format(TABLE_NAME=self.table_name,TASK_ID=self.task_id)
            print sql

    @staticmethod
    def _run(self,result):
        if self.table_name:
            for key,items in enumerate(CmpDict):
                key=str(int(key)+1)
                print key
                if key==str(1):
                    for i in (0,1):
                        flag=i
                        t=TotalFeeCount(self.table_name,flag)
                        ret=t.run()
                        if len(ret)<2:
                            raise Exception('Count %s Info Failed'%(items))
                        result[items]=ret
                elif key==str(2):
                    t= ConsistentFeeCount(self.table_name)
                    t.run()
                    result[items]=ret
                elif key==str(3):
                    t= OldMoreFeeCount(self.table_name)
                    t.run()
                    result[items]=ret
                elif key==str(4):
                    t= NewMoreFeeCount(self.table_name)
                    t.run()
                    result[items]=ret
                elif key==str(5):
                    t= UnconsistentFee(self.table_name)
                    t.run()
                    result[items]=ret
                elif key==str(6):
                    t= UnconsistentCount(self.table_name)
                    t.run()
                    result[items]=ret
                else:
                    raise TypeError('Not Support current type')
        else:
            raise Exception('current table name is null')

    def run(self):

        table=self.build_table()
        content = self.build_compare_content(table)
        if content:
            for k in content:
                ret = k.run()
                if k in self.result_dic:
                    self.result_dic[k] = ret
        return self.result_dic
    @contextmanager
    def clean(self):
        if self.table_name:
            exist_sql = '''select count(*) from {}'''.format(self.table_name)
            print exist_sql

            delete_sql = '''delete from {table} where task_id={task}'''.format(table=self.table_name, task=102)
            print delete_sql

        yield
        insert_sql = '''insert into table {table} VALUES ({values})'''.format(table=self.table_name, values=None)
        print insert_sql



    def update_result(self,result):

        #with ResultClean(self.table_name):
            #print 'process result'
        with self.clean() :
            self.result_dic=result
            self.ProcessToReport()

        if self.result_dic:
            for k,v in enumerate(self.result_dic):
                print k,v
        return True

    def build_compare_content(self,table):
        raise NotImplementedError

class Manager(object):
    def __init__(self,threadlst):
        self.threadlst=threadlst

    def start(self):
        t = threading.Thread(target=self.run)
        t.start()
        t.join()

    def run(self):
        for t in self.threadlst:
            t.start()
        for t in self.threadlst:
            t.join()
        while self.threadlst != []:
            for t in self.threadlst:
                if t.isAlive():
                    print t.name
                    print ('%s is running ') % (t.name)
                    print ('%s is alive') % (t.name)
                else:
                    self.threadlst.remove(t)
            print 'current threads %s' % (str(self.threadlst))

class ThreadManager(threading.Thread):
    def __init__(self,threadlst):
        super(ThreadManager,self).__init__()
        self.threadlst=threadlst
        self.lock=threading.RLock()

    def run(self):
        for t in self.threadlst:
            t.start()

        for t in self.threadlst:
            t.join()
        self.join()

        while self.threadlst != []:
            for t in self.threadlst:
                print t.name
                print ('%s is running ') % (t.name)
                if t.isAlive():
                    print ('%s is alive') % (t.name)
                else:
                    self.threadlst.remove(t)

class DbCompare(Compare):
    def __init__(self,table_name,flag,task_id):
        Compare.__init__(self,table_name,flag,task_id)
        self.type=1
        self.manager=None
        self.lock=threading.RLock()


    def construct_compare(self):
        self.builder=dbcmp.Builder(self.table_name)
        self.builder.config_dbcompare()

    @property
    def compare(self):
        return self.builder

    def build_compare_content(self,table_name):
        if self.flag==self.e.old:
            return dict(
        totalfeecount = dbcmp.TotalFeeCount(table_name,self.flag))
        else:
            content = dict(
                totalfeecount=dbcmp.TotalFeeCount(table_name, self.flag),
                consistentfeecount=dbcmp.ConsistentFeeCount(table_name),
                oldmorefeecount=dbcmp.OldMoreFeeCount(table_name),
                newmorefeecount=dbcmp.NewMoreFeeCount(table_name),
                unconsistentcount=dbcmp.UnconsistentCount(table_name),
                unconsistentfee=dbcmp.UnconsistentFee(table_name, self.flag)
            )
            return content



    def run(self):
        global result
        result={}
        try:
            table=self.build_table()
            #get table from busi if busi
            contents=[self.build_compare_content(i) for i in table]


        except Exception as e:
            raise Exception('get compare content is failed')
        for c in contents:
            print c
            for d in c:
                print d
                func = c[d]
                key = func.__str__()
                t = threading.Thread(target=func.run, args=(result, key))
                self.threadlst.append(t)



        self.manager=Manager(self.threadlst)
        #self.manager=ThreadManager(self.threadlst)
        #self.manager.setName('Manager')

        self.manager.run()
        #self.manager.run()
        return result


class MdbCompare(Compare):
    def __init__(self,table_name,task_id):
        Compare.__init__(table_name,task_id)

    def run(self,*agrs,**kwargs):
        global result
        result = {}
        try:
            table = self.build_table()[0]
            content = self.build_compare_content(table)
        except Exception as e:
            raise Exception('get compare content is failed')
        for c in content:
            func = content[c]
            key = func.__str__()
            t = threading.Thread(target=func.run, args=(result, key))
            self.threadlst.append(t)

        self.manager = Manager(self.threadlst)
        # self.manager=ThreadManager(self.threadlst)
        # self.manager.setName('Manager')

        self.manager.run()
        # self.manager.run()
        return result

    def build_compare_content(self,table_name):
        if self.flag==self.e.old:
            return dict(
            totalfeecount=dbcmp.TotalFeeCount(table_name, self.flag))
        else:
            content = dict(
                totalfeecount=dbcmp.TotalFeeCount(table_name, self.flag),
                consistentfeecount=dbcmp.ConsistentFeeCount(table_name),
                oldmorefeecount=dbcmp.OldMoreFeeCount(table_name),
                newmorefeecount=dbcmp.NewMoreFeeCount(table_name),
                unconsistentcount=dbcmp.UnconsistentCount(table_name),
                unconsistentfee=dbcmp.UnconsistentFee(table_name, self.flag)
            )
            return content


class UploadCompare(Compare):
    def __init__(self,table_name,flag,task_id):
        Compare.__init__(table_name,flag,task_id)

    def build_compare_content(self,table_name):
        table_name = self.build_table()[0]
        content = dict(
            totalfeecount=dbcmp.TotalFeeCount(table_name, self.flag)
        )
        return content

    def run(self):
        global result
        result = {}
        try:
            table = self.build_table()[0]
            content = self.build_compare_content(table)
        except Exception as e:
            raise Exception('get compare content is failed')
        for c in content:
            func = content[c]
            key = func.__str__()
            t = threading.Thread(target=func.run, args=(result, key))
            self.threadlst.append(t)

        self.manager = Manager(self.threadlst)
        # self.manager=ThreadManager(self.threadlst)
        # self.manager.setName('Manager')

        self.manager.run()
        # self.manager.run()
        return result
