#--*-- coding:utf-8 --*--
CmpDict={}
from apps.dbcmp import *


class ComparasionResult():

    def __init__(self,table_name):
        self.table_name=table_name
        self.result_dic={ v:k for k,v in CmpDict.items()}



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


class Compare(ComparasionResult):
    def __init__(self,table_name):
        ComparasionResult.__init__(table_name)
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
        try:
            busi=self.get_busi_type()
            day=self.get_curr_days()
        except Exception:
            raise  Exception('build table failed cause by day or busi')

        if any([busi,day]):
            table_str=self.table_name+day
            if busi=='gsm' or busi=='ggprs':
                all_region_codes=[]
                return [table_str+i for i in all_region_codes]
            else:
                return [table_str]
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

    def _clean(self):
        if self.table_name:
            sql='''delete from {TABLE_NAME} where task_id={TASK_ID} '''.format(TABLE_NAME=self.table_name,TASK_ID='')

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
        content = self.build_compare_content()
        if content:
            for k in content:
                ret = k.run()
                if k in self.result_dic:
                    self.result_dic[k] = ret
        return self.result_dic

    def update_result(self):
        if self.result_dic:
            for k in self.result_dic:
                update_sql = ''''''
                update_sql.execute()
        return True

    def build_compare_content(self):
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

    def build_compare_content(self):
        content=dict(
        totalfeecount = TotalFeeCount(self.table_name),
        consistentfeecount = ConsistentFeeCount(self.table_name),
        oldmorefeecount = OldMoreFeeCount(self.table_name),
        newmorefeecount = NewMoreFeeCount(self.table_name),
        unconsistentcount = UnconsistentCount(self.table_name),
        unconsistentfee = UnconsistentFee(self.table_name)
        )
        return content




class MdbCompare(Compare):
    def __init__(self,table_name):
        Compare.__init__(table_name)

    def run(self,*agrs,**kwargs):
        self.build_table(cls=Compare)

    def build_compare_content(self):
        content = dict(
            totalfeecount=TotalFeeCount(self.table_name),
            consistentfeecount=ConsistentFeeCount(self.table_name),
            oldmorefeecount=OldMoreFeeCount(self.table_name),
            newmorefeecount=NewMoreFeeCount(self.table_name),
            unconsistentcount=UnconsistentCount(self.table_name),
            unconsistentfee=UnconsistentFee(self.table_name)
        )
        return content



class UploadCompare(Compare):
    def __init__(self,table_name):
        Compare.__init__(table_name)


    def build_compare_content(self):
        content = dict(
            totalfeecount=TotalFeeCount(self.table_name)
        )
        return content

