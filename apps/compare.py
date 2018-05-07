#--*-- coding:utf-8 --*--
CmpDict={}
from apps.dbcmp import *

class Compare(object):
    def __init__(self,table_name):
        self.table_name=table_name#table_name:dr_ggprs_770_20170809
        self.builder=None

    def checkTable(self):
        if any([self.table_name]):
            return True

    def construct_compare(self,*args,**kwargs):
        raise NotImplementedError


    def build_table(self):
        '''
        @:parameter self.table de_gsm_$region_code_$
        :return: tablenamelst like [dr_gsm_770_201804]
        '''
        busi=self.get_busi_type()
        day=self.get_curr_days()
        if busi:
            table_str=self.table_name+
            if busi=='gsm' or busi=='ggprs':
                all_region_codes=[]
                return [i for i in all_region_codes]
            else:

                table_name=
                return table_name

    def get_busi_type(self):
        if self.table:
            busi=''
            return busi

    def get_curr_days(self):
        now_day=datetime.datetime.now().strftime('%Y%m')
        return now_day

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

    def run(self,*agrs,**kwargs):
        raise NotImplementedError


class DbCompare(Compare):
    def __init__(self,table_name):
        Compare.__init__(table_name)
        self.type=1

    def construct_compare(self):
        self.builder=Builder(self.table_name)
        self.builder.config_dbcompare()


    @property
    def compare(self):
        return self.builder




class MdbCompare(Compare):
    def __init__(self,table_name):
        Compare.__init__(table_name)

class UploadCompare(Compare):
    def __init__(self,table_name):
        Compare.__init__(table_name)
