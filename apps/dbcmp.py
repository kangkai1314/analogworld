#--*-- coding:utf-8 --*--

'''
db比对算法

'''
import threading

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

    def do_preconditon(self):
        raise NotImplementedError

    def generate(self,*args,**kwargs):
        raise NotImplementedError

    def run(self,*args,**kwargs):
        raise NotImplementedError

    def check(self):
        raise NotImplementedError

class TotalFeeCount(Sql):
    type=1#话单和费用总数

    def __init__(self,table_name,flag):
        Sql.__init__(table_name)
        self.flag=flag

    def generate(self,dbuser):
        base_sql='''select count(*), sum(charge1+charge2+charge3+charge4) from {DB_USER}.{TABLE_NAME}'''.format(DB_USER=dbuser,TABLE_NAME=self.table_name)
        return self.flag,base_sql
    def run(self,sql):
        if sql:
            ret=sql.execute()
            return self.flag,ret


class ConsistentFeeCount(Sql):
    type=2#一致的话单数和费用

    def __init__(self,table_name):
        Sql.__init__(table_name)

    def generate(self,*args,**kwargs):
        base_sql=''''''
        return base_sql

class OldMoreFeeCount(Sql):
    type=3#旧有新无

    def __init__(self,table_name):
        Sql.__init__(table_name)

    def generate(self,*args,**kwargs):
        base_sql=''''''
        return base_sql

class NewMoreFeeCount(Sql):
    type=4

    def __init__(self,table_name):
        Sql.__init__(table_name)


    def generate(self,*args,**kwargs):
        base_sql=''''''
        return base_sql

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

class Compare():
    def __init__(self,table_name):
        self.table_name=table_name#table_name:dr_ggprs_770_20170809

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        print 'Compare function is exit'



    def _clean(self):
        if self.table_name:
            sql='''delete from {TABLE_NAME} where task_id={TASK_ID} '''.format(TABLE_NAME=self.table_name,TASK_ID='')

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

    def run(self,result):
        cmp_result = ComparasionResult(self.table_name)
        t=threading.Thread(target=self._run,args=(cmp_result,))
        t.start()



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






def main():
    table_name='dr_gsm_701_201804'
    cmp=Compare(table_name)
    cmp.run('pass')


if __name__ == '__main__':
    main()



























