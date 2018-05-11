#--*-- coding:utf-8 --*--
CmpDict={}

import dbcmp
import datetime
import threading

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
    def __init__(self,table_name,flag):
        ComparasionResult.__init__(self,table_name)
        self.table_name=table_name#table_name:dr_ggprs_770_20170809
        self.builder=None
        self.flag=flag
        self.threadlst=[]


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
                all_region_codes=['770']
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
        table=self.build_table()
        content = self.build_compare_content(table)
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
    def __init__(self,table_name,flag):
        Compare.__init__(self,table_name,flag)
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
        content=dict(
        totalfeecount = dbcmp.TotalFeeCount(table_name,self.flag),
        consistentfeecount = dbcmp.ConsistentFeeCount(table_name),
        oldmorefeecount = dbcmp.OldMoreFeeCount(table_name),
        newmorefeecount = dbcmp.NewMoreFeeCount(table_name),
        unconsistentcount = dbcmp.UnconsistentCount(table_name),
        unconsistentfee = dbcmp.UnconsistentFee(table_name,self.flag)
        )
        return content

    def run(self):
        global result
        result={}
        try:
            table=self.build_table()[0]
            content=self.build_compare_content(table)
        except Exception as e:
            raise Exception('get compare content is failed')
        for c in content:
            func=content[c]
            key=func.__str__()
            t=threading.Thread(target=func.run,args=(result,key))
            self.threadlst.append(t)

        self.manager=Manager(self.threadlst)
        #self.manager=ThreadManager(self.threadlst)
        #self.manager.setName('Manager')

        self.manager.run()
        #self.manager.run()
        print result


class MdbCompare(Compare):
    def __init__(self,table_name):
        Compare.__init__(table_name)

    def run(self,*agrs,**kwargs):
        self.build_table(cls=Compare)

    def build_compare_content(self,table_name):

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
    def __init__(self,table_name,flag):
        Compare.__init__(table_name,flag)

    def build_compare_content(self,table_name):
        table_name = self.build_table()[0]
        content = dict(
            totalfeecount=dbcmp.TotalFeeCount(table_name, self.flag)
        )
        return content
