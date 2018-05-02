#!/usr/bin/env python
import os, sys
from multiprocessing import *

from regress.compare.base import *
from regress.compare.cond import *
from regress.compare.selector import *
from regress.compare.totalstat import *
from utils.util import *
from utils.command import *
from utils.dzutils import *
from regress.defines import BUSI_COMPARE_TYPE_MAP, STATUS_MAP, ALL_REGION_ALL_PROV
from apps.dbcmp import CmpTableObj,Compare

# at_tc_dbcmp_tabsinfo :keys,diff_recs,sum_recs,wheres,groups,orders,tabdefines,tabalias
# tabdefines just like: one_tablename_with_vars, some_tableName_with_union, some_sql_with_union_brackets
# 1,dr_gprs_$regioncode_$day
# 2,dr_gprs_770 union dr_gprs_771
# 3,(select * from dr_gprs_$regioncode_$day) union all (select * from dr_gprs_$regioncode_$lastday) union all (select * from dr_gprs_$regioncode_$nextday)
# 4,(select a.*,b.* from dr_gprs_$regioncode_$day a, ca_account_$mod b where a.acct_id=b.acct_id) c

# at_tc_dbcmp_scene_detail
# 0 scene all,no where sql
# 1000-2000 scene for product_offer_id
class GlobalVar():
    FEA_CHAR_ARRAY = [['to_char(', ')', 1, ',']]
    ORA_FUNC_PARATYPE = {'to_char': ['C', 'K'], 'decode': ['K', 'C', 'C', 'K'],
                         'sumall4': ['K', 'K', 'K', 'K'], 'sumall2': ['K', 'K'],
                         'sumall3': ['K', 'K', 'K'], 'max': ['K'], 'min': ['K']
                         }
    ORA_FUNC_RETTYPE = {'to_char': 'varchar', 'decode': 'p2'}
    key_split_char = ','
    key_split_char_insel = '?'
    key_map_conn = 'atptest/atptest@oel1247'
    CHAR_INSTEAD_OF_CNT = 'records'
    id_splitor = '__'

    def __init__(self):
        pass

    @staticmethod
    def getKeyPoses(funcname):
        poslst = []
        if funcname.lower() in GlobalVar.ORA_FUNC_PARATYPE.keys():
            plst = GlobalVar.ORA_FUNC_PARATYPE[funcname]
            for i in range(0, len(plst)):
                if plst[i] == 'K':
                    poslst.append(i)
        return poslst

    @staticmethod
    def getRetType(funcname):
        if funcname.lower() in GlobalVar.ORA_FUNC_RETTYPE.keys():
            return GlobalVar.ORA_FUNC_RETTYPE[funcname.lower()]
        return 'varchar2'


def getrelcolname(colname):
    if colname == '':
        return ''
    lst = colname.split('.')
    if len(lst) == 2:
        return lst[1]
    return lst[0]


def getrelcols(collst):
    L = []
    for item in collst:
        it = getrelcolname(item)
        if it != '':
            L.append(it)
    return L


def unioncolumns(cols1, cols2, split_char, flag=1):
    if cols1 == '' and cols2 == '':
        return ''
    if cols1 == '':
        return cols2
    if cols2 == '':
        return cols1
    lst = cols2.split(split_char)
    lst1 = cols1.split(split_char)
    if flag == 1:
        lst2 = getrelcols(lst1)
        lst3 = getrelcols(lst)
    else:
        lst2 = lst1
        lst3 = lst
    cols = cols1
    for item in lst3:
        if len(item.strip()) > 0 and item.strip() not in lst2:
            cols += split_char + item
    prints('unioncolumns:', cols1, '   ', cols2, '  ', cols)
    return cols


g_testdbenvm = None


def cuteach(lst, lastchars):
    lst1 = []
    for it in lst:
        it1 = str(it)[:-lastchars]
        if it1 not in lst1:
            lst1.append(it1)
    return lst1


class SqlField():
    def __init__(self, field_name):
        self.FEA_CHAR_ARRAY = GlobalVar.FEA_CHAR_ARRAY
        self.key_split_char = GlobalVar.key_split_char
        self.field_name = field_name
        self.field_with_table = self.field_name
        self.isfunc = False
        self.funcname = ''
        self.funcparas = []
        self.ori_field_name = self.convertfieldname()

        self.ori_field_names = []
        self.IDname = self.field_name

    def setFunc(self, funcname, paralst=[]):
        self.isfunc = True
        self.funcname = funcname
        self.funcparas = paralst
        poslst = GlobalVar.getKeyPoses(self.funcname)
        for i in range(0, len(self.funcparas)):
            if i in poslst:
                if self.funcparas[i] not in self.ori_field_names:
                    self.ori_field_names.append(self.funcparas[i])
        ori_field_names = ''
        for it in self.ori_field_names:
            ori_field_names += it.split('.')[-1] + GlobalVar.id_splitor
        ori_field_names = ori_field_names.rstrip(GlobalVar.id_splitor)
        self.IDname = ori_field_names

    def setfeachararray(self, fea_char_array=GlobalVar.FEA_CHAR_ARRAY):
        self.FEA_CHAR_ARRAY = fea_char_array

    def setkeysplitchar(self, key_split_char=GlobalVar.key_split_char):
        self.key_split_char = key_split_char

    @staticmethod
    def splitkeys(keys, fea_chars_array=GlobalVar.FEA_CHAR_ARRAY, key_split_char=GlobalVar.key_split_char):
        lst = []
        lst1 = []
        lst1.append(keys)
        while len(lst1) > 0:
            notsplit = False
            for keys1 in lst1:
                if keys1 == key_split_char:
                    lst1.remove(keys1)
                    continue
                bsplit = False
                for fea_chars in fea_chars_array:
                    fea_char = fea_chars[0]
                    fea_end = fea_chars[1]
                    idx = keys1.find(fea_char)
                    if idx > -1:
                        if idx > 0:
                            lst1.append(keys1[:idx])
                        idx2 = keys1[idx:].find(fea_end)
                        lst.append(keys1[idx:idx + idx2 + 1])
                        if len(keys1) > idx + idx2 + 1:
                            lst1.append(keys1[idx + idx2 + 1:])
                        bsplit = True
                if bsplit == True:
                    lst1.remove(keys1)
                    notsplit = True
                else:
                    break
            if notsplit == False:
                break
        for key in lst1:
            lst2 = key.split(key_split_char)
            for item in lst2:
                if len(item) > 0:
                    lst.append(item)
        return lst

    def convertfieldname(self):
        new_field_name = self.field_name
        for fea_chars in self.FEA_CHAR_ARRAY:
            fea_char = fea_chars[0]
            fea_end = fea_chars[1]
            ipos = int(fea_chars[2])
            idx1 = new_field_name.find(fea_char)
            idx2 = new_field_name.find(fea_end)

            if idx1 > -1 and idx2 > -1:
                new_field_name = new_field_name[idx1 + 1:idx2]
                newlst = new_field_name.split(fea_chars[3])
                if len(newlst) > ipos:
                    new_field_name = newlst[ipos]
                    break

        return new_field_name

    @staticmethod
    def convertlst2lst(lst, fea_chars_array=GlobalVar.FEA_CHAR_ARRAY):
        lst1 = []
        for item in lst:
            fitem = SqlField(item)
            lst1.append(fitem.ori_field_name)
        return lst1

    @staticmethod
    def getrealkeylst(keys, fea_chars_array=GlobalVar.FEA_CHAR_ARRAY):
        lst = SqlField.splitkeys(keys, fea_chars_array, GlobalVar.key_split_char)
        lst1 = SqlField.convertlst2lst(lst)

        return lst1

    def isspecialkey(self):
        for fea_chars in self.FEA_CHAR_ARRAY:
            fea_char = fea_chars[0]
            fea_end = fea_chars[1]
            if self.field_name.find(fea_char) > -1 and key.find(fea_end) > -1:
                return True
        return False

    def addkeywithtab_old(self, tablename, dbObj, use_to_char=True):
        if self.isspecialkey() == True:
            k1 = self.ori_field_name
            k2 = tablename.strip() + '.' + k1.strip()
            self.field_with_table = self.field_name.replace(k1, k2)
        else:
            if use_to_char:
                self.field_with_table = dbObj.gettocharstr(tablename.strip() + '.' + self.field_name.strip())
            else:
                self.field_with_table = tablename.strip() + '.' + self.field_name.strip()  # #'to_char('+tab.strip()+'.'+key.strip()+')'
        return self.field_with_table

    def addkeywithtab(self, tablename):
        if self.isfunc == True:
            self.field_with_table = self.field_name.strip()
            for it in self.ori_field_names:
                it1 = it.split('.')[-1].split()[0]
                self.field_with_table = self.field_with_table.replace(it, tablename.strip() + '.' + it1.strip())
        else:
            self.field_with_table = tablename.strip() + '.' + self.field_name.strip()  # #'to_char('+tab.strip()+'.'+key.strip()+')'
        return self.field_with_table

    def addkeywithtabfea(self, tablename, feastr):
        return self.addkeywithtab(tablename) + ' ' + self.IDname + feastr

    def getdefaultval(self, isnum=0, dtype=''):
        if self.isfunc == False:
            if isnum == 0:
                if dtype.find('number') > -1 or dtype.find('short') > -1 or dtype.find('long') > -1:
                    return "0"
                return "'0'"
            else:
                return "0"
        else:
            rettype = GlobalVar.getRetType(self.funcname)
            if rettype[0].lower() == 'p':
                if rettype[1:].isdigit():
                    rettype = 'number'
                elif rettype[1:].isalpha():
                    rettype = 'varchar'
            if rettype.lower().find('number') > -1 or rettype.lower().find('double') > -1 or rettype.lower().find(
                    'short') > -1:
                return "0"
            else:
                return ''

    def getdefaultval_old(self, isnum=0):
        if self.isspecialkey() == True:
            return ""
        elif isnum == 0:
            return "'0'"
        else:
            return "0"


class FieldNameMap():
    def __init__(self, user_field_name, table_name, splitchar=','):
        self.user_field_name = user_field_name
        self.table_name = table_name
        self.splitchar = splitchar
        self.table_field_name = self.convertkeys(self.user_field_name, self.table_name, self.splitchar)

    @staticmethod
    def getkeymap():
        global key_map
        if key_map == {}:
            prints('getkeymap:', GlobalVar.key_map_conn)
            dbobj = DBObj(GlobalVar.key_map_conn)
            tabname = 'at_tc_dbcmp_keymap'
            sql = 'select tabname,key_old,key_new from %s' % (tabname)
            lst, msg = dbobj.execute(sql)
            if len(lst) == 0:
                key_map = {'null': {}}
                return
            for item in lst:
                tname = item[0].strip()
                kold = item[1].strip()
                knew = item[2].strip()
                if tname not in key_map.keys():
                    key_map[tname] = {}
                key_map[tname][kold] = knew
        prints('get_key_map:', key_map)
        return

    def convertkey(self, key, tabname):
        global key_map
        FieldNameMap.getkeymap()
        newkey = key
        found = 0
        notfound = []
        for tname in key_map.keys():
            if tname == 'null':
                continue
            if tname.lower() == 'common':
                continue
            if tabname in tname.split(','):
                km = key_map[tname.strip()]
                for kk in key.split('_'):
                    if kk.strip() in km.keys():
                        newkey = newkey.replace(kk, km[kk.strip()])
                        found = 1
                    else:
                        notfound.append(kk)
                break
                if key.strip() in km.keys():
                    newkey = km[key.strip()]
                    found = 1
                    break
        if notfound != []:
            if 'common' in key_map.keys():
                km = key_map['common']
                for kk in notfound:
                    if kk.strip() in km.keys():
                        newkey = newkey.replace(kk, km[kk.strip()])

                        # if key.strip() in km.keys():
                        #    newkey = km[key.strip()]
                        prints('dbcompare:convertkey:common:%s,%s,%s' % (tabname, key, newkey))
        return newkey

    def convertkeys(self, keys, tabname, splitchar):
        lst = keys.split(splitchar)
        str = ''
        for item in lst:
            str += self.convertkey(item, tabname) + splitchar
        str = str[:-len(splitchar)]
        return str


class SqlItem():
    DIFF_ITEM_TYPE_MAP = {"KeyItem": 0, "DiffItem": 1, "NumericDiffItem": 2, "SumItem": 3, "CountItem": 4,
                          "RateItem": 5, "MinusItem": 6}

    @staticmethod
    def removeDottedAlias(sql):
        flags = ['+', '-', '*', '/', ' ', '\n', '(', ',', ';', "'", '"']
        itlst = sql.split('.')
        if len(itlst) == 1:
            return sql
        for it in flags:
            if it in itlst[0]:
                idx = itlst[0].rfind(it)
                if itlst[0][idx + 1:].find(' ') == -1 and itlst[0][idx + 1:].find(',') == -1:
                    return itlst[0][:idx + 1] + itlst[1]
        return itlst[1]

    def __init__(self, recs, dbObj, type=0, key_prefix=''):
        self.recs = recs
        self.dbObj = dbObj
        self.type = type
        self.isnum = 0
        strrec = ''
        self.fieldobjs = []
        self.typemap = {}
        if self.recs != '':
            for it in self.recs.split(GlobalVar.key_split_char):
                if it == '':
                    continue
                removedkey = it.split('.')[-1]
                removedkey = SqlItem.removeDottedAlias(it)
                strrec += key_prefix + removedkey.split()[0].lstrip() + GlobalVar.key_split_char
            if strrec != '':
                self.recs = strrec[:-len(GlobalVar.key_split_char)]
        recs1 = recs
        if recs1 != '':
            idx = 0
            idx_lst = []
            while idx > -1:
                idx = recs1.find('(', idx + 1)
                idx1 = recs1.find(')', idx + 1)
                if idx == -1 or idx1 == -1:
                    break
                idx_lst.append([idx, idx1, recs1[idx + 1:idx1]])

                recs1 = recs1[:idx + 1] + ' ' * (idx1 - idx - 1) + recs1[idx1:]
                idx = idx1
            i = 0
            for it in recs1.split(GlobalVar.key_split_char):
                idx1 = it.find('(')
                idx2 = it.find(')')
                if idx1 > -1 and idx2 > -1:

                    it1 = it[:idx1 + 1] + idx_lst[i][2] + it[idx2:]
                    fo = SqlField(it1)
                    lst0 = idx_lst[i][2].split(',')
                    lst1 = []
                    for uit in lst0:
                        # lst1.append(uit.split('.')[-1].split()[0])
                        lst1.append(uit.split()[0])
                    fo.setFunc(it[:idx1], lst1)
                    self.fieldobjs.append(fo)
                    i += 1
                else:
                    if it == '':
                        fo = SqlField('')
                        self.fieldobjs.append(fo)
                    else:
                        fo = SqlField(it.split('.')[-1].split()[0])
                        self.fieldobjs.append(fo)

    def getIDKeys(self):
        idnames = ''
        for fo in self.fieldobjs:
            idnlst = fo.IDname.split(GlobalVar.id_splitor)
            for id in idnlst:
                if idnames.find(id) == -1:
                    idnames += id + ','
        idnames = idnames.rstrip(',')
        return idnames

    def settypemap(self, typemap):
        self.typemap = typemap

    def getkeys(self, tablename):
        keystr = ''
        for fo in self.fieldobjs:
            keystr += fo.addkeywithtab(tablename) + ','
        keystr = keystr.rstrip(',')
        return keystr

    def getkeys_old(self, tablename):
        keylist = self.recs.split(GlobalVar.key_split_char)
        if tablename.strip() == '':
            return self.recs
        newkeys = ''
        for item in keylist:
            newkeys += tablename + '.' + item + ','
        if len(newkeys) > 0:
            newkeys = newkeys[:-1]
        else:
            newkeys = "''"
        return newkeys

    def getkeyswithfea(self, tablename, feastr):
        keystr = ''
        for fo in self.fieldobjs:
            keystr += fo.addkeywithtabfea(tablename, feastr) + ','
        keystr = keystr.rstrip(',')
        return keystr

    def getkeyswithfea_old(self, tablename, feastr):
        keylist = self.recs.split(GlobalVar.key_split_char)
        newkeys = ''
        for item in keylist:
            newkeys += tablename + '.' + item + ' ' + item + feastr + ','
        if len(newkeys) > 0:
            newkeys = newkeys[:-1]
        else:
            newkeys = "''"
        return newkeys

    @staticmethod
    def convertnullstr2str(str):
        if str == '':
            str = "''"
        return str

    def generateitem4cmp_old(self, tablename, item):
        # modified  item like a.a1 alias   and return b.a.a1 alias ,but b.a1 expected
        fitem = SqlField(item.split('.')[-1].split()[0])
        if tablename == '':
            k1 = fitem.getdefaultval(self.isnum, '')
        else:
            k1 = fitem.addkeywithtab(tablename)
        return k1

    def generateitem4cmp(self, tablename, fitem):
        # modified  item like a.a1 alias   and return b.a.a1 alias ,but b.a1 expected
        if tablename == '':
            dtype = ''
            if not fitem.isfunc:
                for tk in self.typemap.keys():
                    tkk = tk.split('.')[-1]
                    if fitem.field_name.lower() == tkk.lower() or fitem.field_name.lower() == tk.lower():
                        dtype = self.typemap[tk].lower()
                        break
            k1 = fitem.getdefaultval(self.isnum, dtype)
        else:
            k1 = fitem.addkeywithtab(tablename)
        return k1

    def makediffitem(self, knew, kold, item):
        return ''

    def getnormaldiff(self, tablename):
        return self.getdiffsql(tablename, '')

    def getnegdiff(self, tablename):
        return self.getdiffsql('', tablename)

    def getdiffsql(self, tablename1, tablename2):
        subform = ''
        for fo in self.fieldobjs:
            k1 = self.generateitem4cmp(tablename1, fo)
            k2 = self.generateitem4cmp(tablename2, fo)
            subform += self.makediffitem(k1, k2, fo.IDname)
        if len(subform) > 0:
            subform = subform[:-1]
        return subform

    def getdiffsql_old(self, tablename1, tablename2):
        lst = SqlField.splitkeys(self.recs)  # self.recs.split(SqlField.key_split_char)
        subform = ''

        for item in lst:
            it = item.split('.')[-1].split()[0]
            if it != '':
                k1 = self.generateitem4cmp(tablename1, it)
                k2 = self.generateitem4cmp(tablename2, it)
                subform += self.makediffitem(k1, k2, it)

        if len(subform) > 0:
            subform = subform[:-1]
        return subform

    def getwherecond(self, tablename1, tablename2):
        wherecond = ''
        for fo in self.fieldobjs:
            wherecond += fo.addkeywithtab(tablename1) + ' = ' + fo.addkeywithtab(tablename2) + ' and '
        if wherecond != '':
            wherecond = wherecond[:-len(' and ')]
        # wherecond = wherecond.rstrip(' and ')
        return wherecond

    def getwherecond_old(self, tablename1, tablename2):
        lstk = self.recs.split(key_split_char)
        wherecond = ' '
        for item in lstk:
            it = item.split('.')[-1].split()[0]
            wherecond += tablename1 + '.' + it + ' = ' + tablename2 + '.' + it + ' and '
        wherecond = wherecond[:-len(' and ')]
        return wherecond

    def generatekeyinfo(self, tablename, key_split_char=GlobalVar.key_split_char, fea_char=''):
        sql = ''
        feastr = ''
        if fea_char != '':
            item = fea_char[1:].split('_')[0]
            feastr = FieldNameMap(item, 'fea').table_field_name
        keys = []
        for fo in self.fieldobjs:
            keys.append(fo.IDname)
        sql = self.makedetailinfo(keys, tablename, key_split_char, feastr, fea_char)
        if sql.strip() == '':
            sql = "''"
        sql = sql + ' %s_info' % (feastr)  # (fea_char[1:])
        prints('generatecmpinfo:', sql)
        return sql

    def generatekeyinfo_old(self, tablename, fea_char=''):
        sql = ''
        feastr = ''
        if fea_char != '':
            item = fea_char[1:].split('_')[0]
            feastr = FieldNameMap(item, 'fea').table_field_name
        recs = SqlField.getrealkeylst(self.recs, GlobalVar.FEA_CHAR_ARRAY)
        sql = self.makedetailinfo(recs, tablename, GlobalVar.key_split_char, feastr, fea_char)
        if sql.strip() == '':
            sql = "''"
        sql = sql + ' %s_info' % (feastr)  # (fea_char[1:])
        prints('generatecmpinfo:', sql)
        return sql

    def makedetailinfo(self, keys, tab, split_char, fea_str='', fea_char=''):
        dbtype = self.dbObj.DBTYPE
        sql = ''
        if dbtype == 2:
            sql = ' concat('
        for key in keys:
            if key == '':
                continue
            newkey = FieldNameMap(key, tab).table_field_name + '_' + fea_str
            if dbtype == 1:
                sql += ''' '%s=' || %s%s  ''' % (newkey, key, fea_char)
            else:
                sql += " '%s=', %s ,' ')" % (newkey, key, fea_char)
            if key != keys[-1]:
                if dbtype == 1:
                    sql += '''|| '%s' || ''' % (split_char)
                else:
                    sql += ''' ' %s ',''' % (split_char)
        return sql

    @staticmethod
    def concatdetails(dbtype, detail1, detail2, split_char):
        if detail1 == '':
            return detail2
        if detail2 == '':
            return detail1
        sql = ''
        if dbtype == 1:
            sql = detail1[:detail1.rfind(' ')] + '''|| '%s' || ''' % (split_char) + detail2
        elif dbtype == 2:
            sql = 'concat(' + detail1 + ''' ' %s ',''' % (split_char) + detail2 + "')"
        return sql


class KeyItem(SqlItem):
    def __init__(self, recs, dbObj, key_prefix=''):
        SqlItem.__init__(self, recs, dbObj, SqlItem.DIFF_ITEM_TYPE_MAP['KeyItem'], key_prefix)

    def makediffitem(self, knew, kold, itemname):
        return ''

    def getdiffsql(self, tablename1, tablename2):
        return ''

    def getnormaldiff(self, tablename):
        return ''

    def getnegdiff(self, tablename):
        return ''

    def getwherecond(self, tablename1, tablename2):
        return SqlItem.getwherecond(self, tablename1, tablename2)

    def generatekeyinfo(self, tablename, key_split_char=GlobalVar.key_split_char, fea_char=''):
        return SqlItem.generatekeyinfo(self, tablename, key_split_char, fea_char)

    def makedetailinfo(self, keys, tab, split_char, fea_str='', fea_char=''):
        return SqlItem.makedetailinfo(self, keys, tab, split_char, fea_str, fea_char)

    def getkeys(self, tablename):
        return SqlItem.getkeys(self, tablename)

    def getkeyswithfea(self, tablename, feastr):
        return SqlItem.getkeyswithfea(self, tablename, feastr)


class DiffItem(SqlItem):  # KeyItem DiffItem,NumericDiffItem,SumItem,CountItem,RateItem,MinusItem:0,1,2,3,4,5,6
    def __init__(self, recs, dbObj, key_prefix=''):
        SqlItem.__init__(self, recs, dbObj, SqlItem.DIFF_ITEM_TYPE_MAP['DiffItem'], key_prefix)

    def makediffitem(self, knew, kold, itemname):
        if kold == '':
            kold = "''"
        if knew == '':
            knew = "''"
        kdiff = self.dbObj.getconcatstr(kold, "'|'", knew)
        kold = SqlItem.convertnullstr2str(kold)
        knew = SqlItem.convertnullstr2str(knew)
        kdiff = SqlItem.convertnullstr2str(kdiff)
        sql = knew + ' ' + itemname + '_new, ' + kold + ' ' + itemname + '_old, ' + kdiff + ' ' + itemname + '_diff,'
        return sql

    def getdiffsql(self, tablename1, tablename2):  # tablename1,tablename2 default ''
        return SqlItem.getdiffsql(self, tablename1, tablename2)

    def getdiffwithoutsub(self, tablename, flag):
        if flag == '':
            return self.getdiffsql(tablename, '')
        elif flag == '-':
            return self.getdiffsql('', tablename)
        return ''

    def getnormaldiff(self, tablename):
        return SqlItem.getnormaldiff(self, tablename)

    def getnegdiff(self, tablename):
        return SqlItem.getnegdiff(self, tablename)

    def getwherecond(self, tablename1, tablename2):
        return SqlItem.getwherecond(self, tablename1, tablename2)

    def generatekeyinfo(self, tablename, key_split_char=GlobalVar.key_split_char, fea_char=''):
        return SqlItem.generatekeyinfo(self, tablename, key_split_char, fea_char)

    def makedetailinfo(self, keys, tab, split_char, fea_str='', fea_char=''):
        return SqlItem.makedetailinfo(self, keys, tab, split_char, fea_str, fea_char)

    def getkeys(self, tablename):
        return SqlItem.getkeys(self, tablename)

    def getkeyswithfea(self, tablename, feastr):
        return SqlItem.getkeyswithfea(self, tablename, feastr)


class NumericDiffItem(SqlItem):
    def __init__(self, recs, dbObj, key_prefix=''):
        SqlItem.__init__(self, recs, dbObj, SqlItem.DIFF_ITEM_TYPE_MAP['NumericDiffItem'], key_prefix)
        self.isnum = 1
        self.countnow = 0

    def setcountNow(self, countnow):
        self.countnow = countnow

    def makediffitem(self, knew, kold, itemname):
        kdiff = kold + ' - ' + knew  # self.dbObj.getconcatstr(kold,"-",knew)
        kold = SqlItem.convertnullstr2str(kold)
        knew = SqlItem.convertnullstr2str(knew)
        kdiff = SqlItem.convertnullstr2str(kdiff)
        sql = ''
        if self.countnow == 0:
            sql = knew + ' ' + itemname + '_new, ' + kold + ' ' + itemname + '_old, ' + kdiff + ' ' + itemname + '_diff,'
        else:
            sql = 'sum(' + knew + ') ' + itemname + '_new_sum, sum(' + kold + ') ' + itemname + '_old_sum, sum(' + kdiff + ') ' + itemname + '_diff_sum,'
        return sql

    def getdiffwithalias(self, tablename, flag):
        if flag == '':
            return self.getsubformular(tablename, '')
        elif flag == '-':
            return self.getsubformular('', tablename)
        return ''

    def getdiffsql(self, tablename1, tablename2):
        return SqlItem.getdiffsql(self, tablename1, tablename2)

    def getnormaldiff(self, tablename):
        return SqlItem.getnormaldiff(self, tablename)

    def getnegdiff(self, tablename):
        return SqlItem.getnegdiff(self, tablename)

    def getwherecond(self, tablename1, tablename2):
        return SqlItem.getwherecond(self, tablename1, tablename2)

    def generatekeyinfo(self, tablename, key_split_char=GlobalVar.key_split_char, fea_char=''):
        return SqlItem.generatekeyinfo(self, tablename, key_split_char, fea_char)

    def makedetailinfo(self, keys, tab, split_char, fea_str='', fea_char=''):
        return SqlItem.makedetailinfo(self, keys, tab, split_char, fea_str, fea_char)

    def getkeys(self, tablename):
        return SqlItem.getkeys(self, tablename)

    def getkeyswithfea(self, tablename, feastr):
        return SqlItem.getkeyswithfea(self, tablename, feastr)


class SumItem(SqlItem):
    def __init__(self, recs, dbObj, key_prefix=''):
        SqlItem.__init__(self, recs, dbObj, SqlItem.DIFF_ITEM_TYPE_MAP['SumItem'], key_prefix)
        self.isnum = 1

    def makediffitem(self, sumnew, sumold, item):
        return 'sum(' + sumnew + ') ' + item + '_new_sum,sum(' + sumold + ') ' + item + '_old_sum,sum(' + sumnew + ') - sum(' + sumold + ') ' + item + '_diff_sum,'

    def getdiffsql(self, tablename1, tablename2):
        return SqlItem.getdiffsql(self, tablename1, tablename2)

    def getsumwithflag(self, tablename, flag=''):
        if flag == '':
            return self.getdiffsql(tablename, '')
        elif flag == '-':
            return self.getdiffsql('', tablename)
        return ''

    def getnormaldiff(self, tablename):
        return SqlItem.getnormaldiff(self, tablename)

    def getnegdiff(self, tablename):
        return SqlItem.getnegdiff(self, tablename)

    def getwherecond(self, tablename1, tablename2):
        return SqlItem.getwherecond(self, tablename1, tablename2)

    def generatekeyinfo(self, tablename, key_split_char=GlobalVar.key_split_char, fea_char=''):
        return SqlItem.generatekeyinfo(self, tablename, key_split_char, fea_char)

    def makedetailinfo(self, keys, tab, split_char, fea_str='', fea_char=''):
        return SqlItem.makedetailinfo(self, keys, tab, split_char, fea_str, fea_char)

    def getkeys(self, tablename):
        return SqlItem.getkeys(self, tablename)

    def getgroups(self, tablename, use_group=True):
        sql = SqlItem.getkeys(self, tablename)
        if not use_group:
            return sql
        return ' group by ' + sql

    def getkeyswithfea(self, tablename, feastr):
        return SqlItem.getkeyswithfea(self, tablename, feastr)


class CountItem(SqlItem):
    def __init__(self, recs, dbObj, key_prefix=''):
        SqlItem.__init__(self, recs, dbObj, SqlItem.DIFF_ITEM_TYPE_MAP['CountItem'], key_prefix)
        self.isnum = 1

    def getcntitem(self, item):
        if item == '0':
            return '0 '
        vname = item.split('.')[-1].split()[0]
        if vname == '1' or vname == '*':
            return 'count ( ' + vname + ') '
        return 'count ( distinct ' + item + ') '

    def makediffitem(self, cntnew, cntold, item):
        cnew = self.getcntitem(cntnew)
        cold = self.getcntitem(cntold)
        cdiff = cnew + ' - ' + cold
        vname = item
        if item == '1' or item == '*':
            vname = GlobalVar.CHAR_INSTEAD_OF_CNT
        return cnew + vname + '_new_sum,' + cold + vname + '_old_sum,' + cdiff + vname + '_diff_sum,'

    def getdiffsql(self, tablename1, tablename2):
        return SqlItem.getdiffsql(self, tablename1, tablename2)

    def getnormaldiff(self, tablename):
        return SqlItem.getnormaldiff(self, tablename)

    def getnegdiff(self, tablename):
        return SqlItem.getnegdiff(self, tablename)

    def getwherecond(self, tablename1, tablename2):
        return SqlItem.getwherecond(self, tablename1, tablename2)

    def generatekeyinfo(self, tablename, key_split_char=GlobalVar.key_split_char, fea_char=''):
        return SqlItem.generatekeyinfo(self, tablename, key_split_char, fea_char)

    def makedetailinfo(self, keys, tab, split_char, fea_str='', fea_char=''):
        dbtype = self.dbObj.DBTYPE
        sql = ''
        if dbtype == 2:
            sql = ' concat('
        for key in keys:
            if key == '':
                continue
            if key == '1' or key == '*':
                key = GlobalVar.CHAR_INSTEAD_OF_CNT
            newkey = FieldNameMap(key, tab).table_field_name + '_' + fea_str
            if dbtype == 1:
                sql += ''' '%s=' || %s%s  ''' % (newkey, key, fea_char)
            else:
                sql += " '%s=', %s ,' ')" % (newkey, key, fea_char)
            if key != keys[-1] and key != GlobalVar.CHAR_INSTEAD_OF_CNT:
                if dbtype == 1:
                    sql += '''|| '%s' || ''' % (split_char)
                else:
                    sql += ''' ' %s ',''' % (split_char)
        return sql

    def getkeys(self, tablename):
        return SqlItem.getkeys(self, tablename)

    def getgroups(self, tablename, use_group=True):
        sql = SqlItem.getkeys(self, tablename)
        if not use_group:
            return sql
        return ' group by ' + sql

    def getkeyswithfea(self, tablename, feastr):
        return SqlItem.getkeyswithfea(self, tablename, feastr)


class RateItem(SqlItem):
    def __init__(self, recs, dbObj, key_prefix=''):
        SqlItem.__init__(self, recs, dbObj, SqlItem.DIFF_ITEM_TYPE_MAP['RateItem'], key_prefix)
        self.isnum = 1

    def makediffitem(self, knew, kold, itemname):
        return ''

    def getdiffsql(self, tablename1, tablename2):
        return ''

    def getnormaldiff(self, tablename):
        return ''

    def getnegdiff(self, tablename):
        return ''

    def getwherecond(self, tablename1, tablename2):
        return SqlItem.getwherecond(self, tablename1, tablename2)

    def generatekeyinfo(self, tablename, key_split_char=GlobalVar.key_split_char, fea_char=''):
        return SqlItem.generatekeyinfo(self, tablename, key_split_char, fea_char)

    def makedetailinfo(self, keys, tab, split_char, fea_str='', fea_char=''):
        return SqlItem.makedetailinfo(self, keys, tab, split_char, fea_str, fea_char)

    def getkeys(self, tablename):
        return SqlItem.getkeys(self, tablename)

    def getkeyswithfea(self, tablename, feastr):
        return SqlItem.getkeyswithfea(self, tablename, feastr)


class MinusItem(SqlItem):
    def __init__(self, recs, dbObj, key_prefix=''):
        SqlItem.__init__(self, recs, dbObj, SqlItem.DIFF_ITEM_TYPE_MAP['MinusItem'], key_prefix)

    def makediffitem(self, knew, kold, itemname):
        return ''

    def getdiffsql(self, tablename1, tablename2):
        return ''

    def getnormaldiff(self, tablename):
        return ''

    def getnegdiff(self, tablename):
        return ''

    def getwherecond(self, tablename1, tablename2):
        return SqlItem.getwherecond(self, tablename1, tablename2)

    def generatekeyinfo(self, tablename, key_split_char=GlobalVar.key_split_char, fea_char=''):
        return SqlItem.generatekeyinfo(self, tablename, key_split_char, fea_char)

    def makedetailinfo(self, keys, tab, split_char, fea_str='', fea_char=''):
        return SqlItem.makedetailinfo(self, keys, tab, split_char, fea_str, fea_char)

    def getkeys(self, tablename):
        return SqlItem.getkeys(self, tablename)

    def getkeyswithfea(self, tablename, feastr):
        return SqlItem.getkeyswithfea(self, tablename, feastr)


class DBLINK():
    def __init__(self, lnkname, dbObj):
        self.dblnk = lnkname
        self.dbinst = ''
        self.dbuser = ''
        self.dbObj = dbObj
        self.getlnk()

    def getlnk(self):
        # if self.dblnk.strip() == '':
        #    return
        sql = "select host,username from dba_db_links where lower(db_link) in ('%s')" % (self.dblnk.lower())
        lst, msg = self.dbObj.execute(sql)
        if len(lst) > 0:
            self.dbuser = lst[0][1]
            self.dbinst = self.get_service_name_from_host_info(lst[0][0])

    def get_service_name_from_host_info(self, host):
        if host.find('(DESCRIPTION') == -1:
            return host
        offset = 0
        idx = host.find('(SERVICE_NAME')
        offset = len('(SERVICE_NAME')
        if idx == -1:
            idx = host.find('( SERVICE_NAME')
            offset = len('( SERVICE_NAME')
        if idx == -1:
            idx = host.find('(SID')
            offset = len('(SID')
        if idx == -1:
            idx = host.find('( SID')
            offset = len('( SID')
        if idx == -1:
            return ''
        idx2 = host.find(')', idx)
        svname = host[idx + offset:idx2]
        svname = svname.split('=')[-1].strip()
        return svname

    def getreallyowner(self, dbuser, table_name):
        if dbuser != '':
            return dbuser
        dblnkstr = ''
        if self.dblnk != '':
            dblnkstr = '@' + self.dblnk
        sql = "select lower(owner) from all_all_tables%s where lower(table_name) = '%s'" % (
        dblnkstr, table_name.lower())
        lst, msg = self.dbObj.execute(sql)
        if len(lst) == 0:
            return ''
        return lst[0][0]

    def issamedbinst(self, dblnk2):
        sql = "select host,username from dba_db_links where dblnk in ('%s','%s')" % (self.dblnk, dblnk2)
        lst, msg = self.dbObj.execute(sql)
        if len(lst) < 2 or self.dbObj.executefailed(msg):
            return False, msg
        if lst[0][0] == lst[1][0]:
            return True, ''
        host1 = lst[0][0]
        host2 = lst[1][0]
        svname1 = self.get_service_name_from_host_info(host1)
        svname2 = self.get_service_name_from_host_info(host2)
        if svname1.lower() == svname2.lower():
            return True, ''

        return False, ''


class TableObj():
    def __init__(self, dbuser, table_name, dblnkstr):
        self.dbuser = dbuser
        self.table_name = table_name
        self.dblnkstr = dblnkstr

    def gettabattr(self):
        if self.dbuser not in ('ud', 'ad'):
            return TestDBEnvManager.TABLE_TYPE_MAP['staticTable']
        return TestDBEnvManager.TABLE_TYPE_MAP['busiTable']


class TestDBEnvManager():
    # four envrionent,  test, old_boss,new_boss, and pd
    # test not old/new/pd,but pd can same to old or new,or old,new,pd is same
    TEST_MANAGET_TYPE_MAP = {"only_one_inst": 0, "two_diff_inst": 1, "same_env": 2}
    ENV_TYPE_MAP = {"old": 0, "new": 1}
    OPERATE_TYPE_MAP = {"in_owner_env": 0, "in_old_env": 1, "in_new_env": 2, "in_test_env": 3}
    TABLE_TYPE_MAP = {"busiTable": 0, "staticTable": 1, "tmpTable": 2}
    ACTUAL_DB_USER_MAP = {}

    def __init__(self, dbObj, projId):
        self.testdbObj = dbObj
        self.projId = projId
        dblnk_old, dblnk_new = self.getdblnkstr()
        self.dblnk1 = DBLINK(dblnk_old, dbObj)
        self.dblnk2 = DBLINK(dblnk_new, dbObj)

        self.table_prefix = self.table_suffix = ''
        self.manager_type = TestDBEnvManager.TEST_MANAGET_TYPE_MAP['only_one_inst']
        if self.dblnk1.dbinst.lower().strip() == self.dblnk2.dbinst.lower().strip():
            if self.dblnk1.dbuser.lower().strip() != self.dblnk2.dbuser.lower().strip():
                self.table_prefix = (self.dblnk1.dbuser.strip(), self.dblnk2.dbuser.strip())
                self.table_suffix = ('', '')
                self.manager_type = TestDBEnvManager.TEST_MANAGET_TYPE_MAP['only_one_inst']
            else:
                self.table_prefix = (self.dblnk1.dbuser.strip(), self.dblnk1.dbuser.strip())
                self.table_suffix = ('', '')
                self.manager_type = TestDBEnvManager.TEST_MANAGET_TYPE_MAP['same_env']
        else:
            self.table_prefix = (self.dblnk1.dbuser.strip(), self.dblnk2.dbuser.strip())
            self.table_suffix = ('@' + self.dblnk1.dbinst.strip(), '@' + self.dblnk2.dbinst.strip())
            self.manager_type = TestDBEnvManager.TEST_MANAGET_TYPE_MAP['two_diff_inst']

            # self.rundbObj = DBObj(dblnk2)

    def getdblnkstr(self):
        sql = "select para_name,para_value from at_sys_base_para where lower(para_name) in ('dblnk_old_%s','dblnk_new_%s')" % (
        str(self.projId), str(self.projId))
        lst, msg = self.testdbObj.execute(sql)
        if len(lst) == 0:
            sql = "select para_name,para_value from at_sys_base_para where lower(para_name) in ('dblnk_old','dblnk_new')"
            lst, msg = self.testdbObj.execute(sql)
        dblnk_old = dblnk_new = ''
        for it in lst:
            if it[0] == 'dblnk_old_%s' % (str(self.projId)) or it[0] == 'dblnk_old':
                dblnk_old = it[1]
            if it[0] == 'dblnk_new_%s' % (str(self.projId)) or it[0] == 'dblnk_new':
                dblnk_new = it[1]
        return dblnk_old, dblnk_new

    def combinateusertab(self, dbuser, table):
        if dbuser.strip() == '':
            return table
        return dbuser + '.' + table

    def gettablename4query(self, table_name, dbuser, env_type, opr_env, taskId, caseId, stepId,
                           use_defined_table_name=True):
        curr_table_name = table_name
        tabobj = TableObj(dbuser, table_name, '')
        table_type = tabobj.gettabattr()
        actual_dbuser = self.getactualdbuser(dbuser, env_type, table_name)
        if table_type in (TestDBEnvManager.TABLE_TYPE_MAP["busiTable"], TestDBEnvManager.TABLE_TYPE_MAP['tmpTable']):
            if use_defined_table_name:
                curr_table_name = self.converttablename2currtablename(table_name, env_type, taskId, caseId, stepId)
        if self.manager_type == TestDBEnvManager.TEST_MANAGET_TYPE_MAP['same_env']:
            if opr_env not in (TestDBEnvManager.OPERATE_TYPE_MAP['in_test_env'],):
                return self.combinateusertab(actual_dbuser, curr_table_name)
            else:
                return self.combinateusertab(actual_dbuser, curr_table_name) + self.table_suffix[0]
        if env_type == TestDBEnvManager.ENV_TYPE_MAP["old"]:
            if opr_env in (
            TestDBEnvManager.OPERATE_TYPE_MAP['in_owner_env'], TestDBEnvManager.OPERATE_TYPE_MAP['in_old_env']):
                return self.combinateusertab(actual_dbuser, curr_table_name)
            else:
                if self.manager_type == TestDBEnvManager.TEST_MANAGET_TYPE_MAP['only_one_inst']:
                    return self.combinateusertab(actual_dbuser, curr_table_name)
                if self.manager_type == TestDBEnvManager.TEST_MANAGET_TYPE_MAP['two_diff_inst']:
                    return self.combinateusertab(actual_dbuser, curr_table_name) + self.table_suffix[env_type]

        elif env_type == TestDBEnvManager.ENV_TYPE_MAP['new']:
            if opr_env in (
            TestDBEnvManager.OPERATE_TYPE_MAP['in_owner_env'], TestDBEnvManager.OPERATE_TYPE_MAP['in_new_env']):
                return self.combinateusertab(actual_dbuser, curr_table_name)
            else:
                if self.manager_type == TestDBEnvManager.TEST_MANAGET_TYPE_MAP['only_one_inst']:
                    return self.combinateusertab(actual_dbuser, curr_table_name)
                if self.manager_type == TestDBEnvManager.TEST_MANAGET_TYPE_MAP['two_diff_inst']:
                    return self.combinateusertab(actual_dbuser, curr_table_name) + self.table_suffix[env_type]
        else:
            return ""
        return ""

    def getactualdbuser(self, dbuser, env_type, table_name):
        if dbuser == '':
            if env_type == TestDBEnvManager.ENV_TYPE_MAP['old']:
                dbuser = self.dblnk1.getreallyowner(dbuser, table_name)
            else:
                dbuser = self.dblnk2.getreallyowner(dbuser, table_name)
            if dbuser != '':
                return dbuser
            return self.table_prefix[env_type]
        if dbuser.lower() not in TestDBEnvManager.ACTUAL_DB_USER_MAP.keys():
            sql = "select dbuser,dbuser_old,dbuser_new from at_tc_dbuser_def where lower(dbuser) = '%s' and proj_id = '%s' " % (
            dbuser.lower(), str(self.projId))
            lst, msg = self.testdbObj.execute(sql)
            if len(lst) == 0:
                sql = "select dbuser,dbuser_old,dbuser_new from at_tc_dbuser_def where lower(dbuser) = '%s' and proj_id = '-1' " % (
                dbuser.lower())
                lst, msg = self.testdbObj.execute(sql)
                TestDBEnvManager.ACTUAL_DB_USER_MAP[dbuser.lower()] = dbuser
                return dbuser
            TestDBEnvManager.ACTUAL_DB_USER_MAP[dbuser.lower()] = lst[0][env_type]

        return TestDBEnvManager.ACTUAL_DB_USER_MAP[dbuser.lower()]

    def converttablename2currtablename(self, table_name, env_type, taskId, caseId, stepId):
        global ALL_REGION_CODES, ALL_REGION_ALL_PROV
        region_code = ''
        if ALL_REGION_CODES == []:
            sql = "select distinct(prov_code) from at_sys_operator_info "
            lstp, msgp = self.testdbObj.execute(sql)
            if len(lstp) > 0:
                ALL_REGION_CODES.extend(ALL_REGION_ALL_PROV[lstp[0][0]])
        for it in ALL_REGION_CODES:
            if table_name.find(str(it)) >= 0 and table_name.find('_' + str(it)) >= 0:
                region_code = it
        daynow = time.strftime('%m%d', time.localtime(time.time()))
        idx = table_name.find(str(region_code))
        curr_tab1 = curr_tab2 = thetablename = ''
        sql = "select curr_table_name from at_tc_table_names_map where task_id = '%s' and tc_id = '%s' and region_code = '%s' and table_name = '%s' and now = '%s' and proj_id = '%s'"
        sql1 = sql % (str(taskId), str(caseId), str(region_code), table_name, str(daynow), str(self.projId))
        lst1, msg1 = self.testdbObj.execute(sql1)
        if len(lst1) > 0:
            curr_tab1 = str(lst1[0][0]) + str(env_type)
            return curr_tab1
        if idx > -1:
            thetablename = table_name[:idx]
        else:
            idx = len(table_name)
            if idx > 20:
                idx = 20
            thetablename = table_name[:idx]
        curr_tab1 = thetablename + str(caseId[-4:]) + str(daynow) + str(region_code)
        # curr_tab1 = thetablename+str(caseId[-4:])+str(diff_sceneId)+str(daynow)
        sql1 = "insert into at_tc_table_names_map (table_name, task_id,tc_id, region_code,curr_table_name,now) \
                 values('%s','%s','%s','%s','%s','%s')" % (
        table_name, str(taskId), str(caseId), str(region_code), curr_tab1, daynow)
        self.testdbObj.execute(sql1)

        return curr_tab1 + str(env_type)

    @staticmethod
    def getTestDBEnvManager(dbObj, projId):
        global g_testdbenvm
        if g_testdbenvm == None:
            g_testdbenvm = TestDBEnvManager(dbObj, projId)
        return g_testdbenvm


class TableNameConvertor():
    def __init__(self, dbObj, projId, taskId, caseId, stepId):
        self.taskId = taskId
        self.projId = projId
        self.stepId = stepId
        self.caseId = caseId
        self.dbObj = dbObj

    def gettablename4env(self, table_name, dbuser, env_type, use_defined_table_name=True):
        testdbenvm = TestDBEnvManager.getTestDBEnvManager(self.dbObj, self.projId)
        return testdbenvm.gettablename4query(table_name, dbuser, env_type, self.taskId, self.caseId, self.stepId,
                                             use_defined_table_name)

    def gettablename4old(self, table_name, dbuser, use_defined_table_name=True):
        return self.gettablename4env(table_name, dbuser, TestDBEnvManager.ENV_TYPE_MAP['old'])

    def gettablename4new(self, table_name, dbuser, use_defined_table_name=True):
        return self.gettablename4env(table_name, dbuser, TestDBEnvManager.ENV_TYPE_MAP['new'], use_defined_table_name)

    def gettablename4statictable(self, table_name, dbuser, env_type):
        return self.gettablename4env(table_name, dbuser, env_type, False)

    def gettaskdayrange(self):
        thetime = int(time.strftime('%Y%m%d', time.localtime(time.time())))
        sql = "select to_char(start_time,'YYYYmmdd') from at_tc_task where task_id = '%s'" % (str(self.taskId))
        lst, msg = self.dbObj.execute(sql)
        if len(lst) == 0 or lst[0] == None or lst[0] == [] or lst[0][0] == None:
            return [thetime]
        return [int(lst[0][0]), thetime]

    def getprobablydays(self):
        dayrange = self.gettaskdayrange()
        return TimeObj.getprobablydays(dayrange)

    def getprobablymonths(self):
        dayrange = self.gettaskdayrange()
        return TimeObj.getprobablymonths(dayrange)

    def getprobablyweeks(self):
        dayrange = self.gettaskdayrange()
        return TimeObj.getprobablyweeks(dayrange)


class SqlTableConvertor():
    # if only use tables in one enviroment, this class may add same dblink and dbuser for each table
    # if use tables in different enviroment, then must add different dblink and dbuser for  tables
    # if table in pd.sd ,then use the same dblink and dbuser,for only has one.
    def __init__(self, sql, tnc):
        # self.tnc = TableNameConvertor(dbObj,projId,taskId,caseId,stepId)
        self.tnc = tnc
        sql1 = SqlAnalyze.redefine_sql_table(sql)
        tablst = SqlAnalyze.gettabnames(sql)
        busiTablst = []
        replace_map = {}

        for it in tablst:
            ilst = it.split('.')
            dbuser = ''
            if len(ilst) == 2:
                dbuser = ilst[0]
            table_name = ilst[-1].split()[0]
            tabobj = TableObj(dbuser, table_name, '')
            if tabobj.gettabattr() == TestDBEnvManager.TABLE_TYPE_MAP['staticTable'] and dbuser != '':
                replace_map[it] = ["ENV_BEGIN " + it + " ENV_END", \
                                   self.tnc.gettablename4statictable(it, dbuser, TestDBEnvManager.ENV_TYPE_MAP['old']), \
                                   self.tnc.gettablename4statictable(it, dbuser, TestDBEnvManager.ENV_TYPE_MAP['new'])]
                continue
            busiTablst.append(it)
            replace_map[it] = ["ENV_BEGIN " + it + " ENV_END", \
                               self.tnc.gettablename4old(it, dbuser), \
                               self.tnc.gettablename4new(it, dbuser)]
        # for iter in busiTablst, init tablenameconvert,and run gettablename4old,gettablename4new,then return two sqls
        sql2 = sql1
        sql3 = sql1
        for it in replace_map.keys():
            sql2 = sql2.replace(replace_map[it][0], replace_map[it][1])
            sql3 = sql3.replace(replace_map[it][0], replace_map[it][2])

        return sql2, sql3


class OriTableDef():
    def __init__(self, normal_table_name, dbuser, dayrange):
        self.region_idx = 0
        self.table_name = normal_table_name
        self.ori_table_lst = []
        self.ori_table_lst = TimeObj.replace_vars_in_tabname_toobj(self.table_name, dayrange)
        self.region_idx = self.table_name.find('$regionCodeEach')
        self.dbuser = dbuser
        self.var_names = []
        self.var_vals = {}

    def get_all_var_names(self):
        if self.var_names != []:
            return self.var_names
        var_names = []
        idx = self.table_name.find('$')
        while idx != -1:
            idx1 = self.table_name.find('_', idx + 1)

            if idx1 == -1:
                var_names.append(self.table_name[idx:])
                break
            else:
                var_names.append(self.table_name[idx:idx1])
            idx = self.table_name.find('$', idx1)

        self.var_names = var_names
        return var_names

    def get_all_var_vals(self, val_name=''):
        self.get_all_var_names()
        var_vals = []
        if val_name != '' and val_name in self.var_vals.keys():
            return self.var_vals[val_name]
        if val_name in self.var_names:
            for tabobj in self.ori_table_lst:
                var_vals.append(tabobj.flagmap[val_name])
        elif val_name == '':
            for val_name in self.var_names:
                var_vals = []
                for tabobj in self.ori_table_lst:
                    var_val = tabobj.flagmap[val_name]
                    if var_val not in var_vals:
                        var_vals.append(var_val)
                self.var_vals[val_name] = var_vals
            return self.var_vals
        self.var_vals[val_name] = var_vals
        return var_vals

    def get_same_union_lst(self):
        ori_tab_ununion_lst = []
        for it in self.ori_table_lst:
            if ori_tab_ununion_lst == []:
                ori_tab_ununion_lst.append([it])
            else:
                found = False
                for sulst in ori_tab_ununion_lst:
                    if sulst[0].has_same_ununion_flagmap(it):
                        sulst.append(it)
                        found = False
                        break
                if not found:
                    ori_tab_ununion_lst.append([it])
        return ori_tab_ununion_lst


class TabClustDef():
    BUSI_PREFIX_MAP = {'50001': 'gsm', '50003': 'gprs'}
    TC_ROOT_TC_MAP = {}

    def __init__(self, user_table_name, dbObj):
        self.user_table_name = user_table_name
        self.busi_id = -1
        self.dbObj = dbObj
        self.keys = None
        self.diff_recs = ''
        self.diff_recs4sum = ''
        self.sumkeys = ''
        self.cntkeys = ''
        self.ratekeys = ''
        self.wherecond = ''
        self.groups = ''
        self.table_define = ''
        self.minus_keys = ''

        self.key_item = None
        self.diff_item = None
        self.numeric_diff_item = None
        self.sum_item = None
        self.count_item = None
        self.rate_item = None
        self.minus_item = None

        self.normal_table_names = []
        self.really_unioned_tables = {}
        self.table_def_withflag = ''
        self.ori_table_names = []
        self.tnc = None
        self.copyoritableflag = True
        self.last_table_define_pair_map = {}

        self.diffScene = []
        self.diff_scene_ids = []

        self.all_probable_days = []

        self.compared_mdb = False
        self.parrelnum = 4

        self.getBusiPrefixMap()

    def getBusiPrefixMap(self):
        if TabClustDef.BUSI_PREFIX_MAP != {}:
            return
        table_name = 'at_tc_busi_prefix_def'
        sql = "select busi_id,busi_table_prefix from %s" % (table_name)
        lst, msg = self.dbObj.execute(sql)
        for it in lst:
            busiId, prefix = it
            if str(busiId) not in TabClustDef.BUSI_PREFIX_MAP.keys():
                TabClustDef.BUSI_PREFIX_MAP[str(busiId)] = prefix
        return

    def setParrelNum(self, n):
        self.parrelnum = n
        return

    def setTNC(self, tnc):
        self.tnc = tnc
        return

    def setbusiId(self, busiId=''):
        if busiId == '':
            if self.busi_id == -1:
                sql = "select busi_id from at_tc_compared_table_selected where  stc_id = %s and lower(table_name) = '%s' " % (
                str(self.tnc.caseId), self.user_table_name)
                lst, msg = self.dbObj.execute(sql)
                if len(lst) == 0:
                    self.busi_id = -1
                    return -1
                self.busi_id = int(lst[0][0])
        self.busi_id = busiId

    def initbylst(self, lst):
        self.keys = lst[0][0]
        self.diff_recs = TabClustDef.getvalexceptnone(lst[0][1])
        # + '|'\
        # + TabClustDef.getvalexceptnone(lst[0][2])
        self.diff_recs4sum = TabClustDef.getvalexceptnone(lst[0][2])
        self.sumkeys = TabClustDef.getvalexceptnone(lst[0][3])
        self.cntkeys = TabClustDef.getvalexceptnone(lst[0][4])
        self.ratekeys = TabClustDef.getvalexceptnone(lst[0][5])
        self.wherecond = TabClustDef.getvalexceptnone(lst[0][6])
        self.groups = TabClustDef.getvalexceptnone(lst[0][7])
        self.table_define = TabClustDef.getvalexceptnone(lst[0][8])
        self.minus_keys = TabClustDef.getvalexceptnone(lst[0][9])
        self.relate_tables = []

        key_prefix = ''
        if self.user_table_name.lower().find('_mdb_') > -1:
            self.compared_mdb = True
        if self.compared_mdb:
            key_prefix = 'aimdb_'
            self.replace_table_def_4mdb()

        self.key_item = KeyItem(self.keys, self.dbObj, key_prefix)
        self.diff_item = DiffItem(self.diff_recs, self.dbObj, key_prefix)
        self.numeric_diff_item = NumericDiffItem(self.diff_recs4sum, self.dbObj, key_prefix)
        self.sum_item = SumItem(self.sumkeys, self.dbObj, key_prefix)
        self.count_item = CountItem(self.cntkeys, self.dbObj, key_prefix)
        self.rate_item = RateItem(self.ratekeys, self.dbObj, key_prefix)
        self.minus_item = MinusItem(self.minus_keys, self.dbObj, key_prefix)
        self.group_item = SumItem(self.groups, self.dbObj, key_prefix)

    def setkeyitemtype(self, dblnk):
        self.key_item.settypemap(self.generatekeytypemap(self.keys, self.key_item, dblnk))
        self.diff_item.settypemap(self.generatekeytypemap(self.diff_recs, self.diff_item, dblnk))
        self.numeric_diff_item.settypemap(self.generatekeytypemap(self.diff_recs4sum, self.numeric_diff_item, dblnk))
        self.sum_item.settypemap(self.generatekeytypemap(self.sumkeys, self.sum_item, dblnk))
        self.count_item.settypemap(self.generatekeytypemap(self.cntkeys, self.count_item, dblnk))
        self.rate_item.settypemap(self.generatekeytypemap(self.ratekeys, self.rate_item, dblnk))
        self.minus_item.settypemap(self.generatekeytypemap(self.minus_keys, self.minus_item, dblnk))

    def generatekeytypemap(self, keys, key_item, dblnk=''):
        typemap = {}
        tablecolmap = {}
        tablst = SqlAnalyze.gettabnames(self.table_define)
        tm = {}
        am = {}

        for t in tablst:
            idx = self.table_define.find(t)
            idx1 = self.table_define.find(',', idx + 1)
            if idx1 == -1:
                idx1 = len(self.table_define)
            tabinfo = self.table_define[idx:idx1].split()
            tab = tabinfo[0]
            alias = ''
            if len(tabinfo) > 1:
                alias = tabinfo[1]
            if alias == '':
                alias = 't_' + str(idx) + "_" + str(idx1)
            tm[alias] = tab
            am[tab] = alias

        tablestr = ''

        if dblnk != '' and dblnk[0] != '@':
            dblnk = '@' + dblnk

        for t in tm.keys():
            tab = tm[t]
            if tab.find('.') == -1:
                if tab.find('$') == -1:
                    tablestr += "lower(table_name) = '" + tab.lower() + "' or "
                else:
                    newtab = StringObj.getstr_replaced_varflag(tab.lower(), '$', '_', '%')
                    newtab = newtab.replace('_', '\_')
                    tablestr += "(lower(table_name) = (select lower(table_name) from all_all_tables" + dblnk + " where lower(table_name) like '" + newtab + "' escape '\\' and rownum<2) or "
            else:
                user = tab[:tab.find('.')]
                tab = tab[tab.find('.') + 1:]
                if tab.find('$') == -1:
                    tablestr += "(lower(table_name) = '" + tab.lower() + "' and lower(owner)='" + user.lower() + "') or "
                else:
                    newtab = StringObj.getstr_replaced_varflag(tab.lower(), '$', '_', '%')
                    newtab = newtab.replace('_', '\_')
                    tablestr += "(lower(table_name) = (select lower(table_name) from all_all_tables" + dblnk + " where lower(table_name) like '" + newtab + "' escape '\\' and rownum<2) and lower(owner)='" + user.lower() + "') or "
        tablestr = tablestr.rstrip(' or ')

        sql = "select lower(column_name),lower(data_type),lower(table_name) from all_tab_columns%s where %s order by table_name" % (
        dblnk, tablestr)
        lst, msg = self.dbObj.execute(sql)
        tcmap = {}
        for item in lst:
            col = item[0]
            type = item[1]
            tab = item[2]
            tablename = ''
            for t in tablst:
                tab2 = t.split('.')[-1].split()[0]
                if StringObj.issamevarstr(tab, tab2, '$', '_'):
                    tablename = tab2
                    break
            if tablename not in tcmap.keys():
                tcmap[tablename] = []
            tcmap[tablename].append(col)

        for it in key_item.fieldobjs:
            if it.isfunc:
                continue
            idx = keys.lower().find(it.field_name.lower())
            if idx == -1:
                continue
            idx1 = keys[:idx].rfind(',')
            idx2 = keys.find(',', idx)
            if idx2 == -1:
                idx2 = len(keys)
            kname = keys[idx1 + 1:idx2]
            aliasinfo = kname.split('.')
            key = aliasinfo[-1]
            alias = ''
            if len(aliasinfo) >= 2:
                alias = aliasinfo[-2]
            tablename = ''
            if alias == '':
                for k in tcmap.keys():
                    if key.lower() in tcmap[k]:
                        tablename = k
                        break
            else:
                tablename = tm[alias]

            if tablename not in tablecolmap.keys():
                tablecolmap[tablename] = []
            tablecolmap[tablename].append(key.lower())

        if len(lst) > 0:
            for item in lst:
                col = item[0]
                type = item[1]
                tab = item[2]
                for tab2 in tablecolmap.keys():
                    if StringObj.issamevarstr(tab, tab2, '$', '_'):
                        if col in tablecolmap[tab2]:
                            for tinfo in am.keys():
                                if tab2.lower() == tinfo.split('.')[-1].split()[0].lower():
                                    alias = am[tinfo]
                                    break
                            if alias.find('t_') == 0:
                                alias = ''
                            thekey = col
                            if alias != '':
                                thekey = alias + '.' + thekey
                            typemap[thekey] = type
                            break
        return typemap

    def replace_table_def_4mdb(self):
        sql1 = "select lower(table_name) from at_tv_tab_define where lower(type) = 'mdb'"
        lst, msg = self.dbObj.execute(sql)
        table_def = self.table_define
        for it in lst:
            if it[0].strip() == '':
                continue
            mdb_tab = ' ' + it[0] + ' '
            if table_def.lower().find(mdb_tab) > -1:
                table_def = table_def.lower().replace(mdb_tab, 'aimdb_' + it[0])
        self.table_define = table_def

        return self.table_define

    def gettaballdef(self, casetype, caseid):
        CASE_TYPE_ALL = -1
        thedbobj = self.dbObj
        sql = '''select lower(keys),lower(diff_recs_detail),lower(diff_recs_sum),lower(sum_keys),
                        lower(cnt_keys),lower(rate_keys),lower(wherecond),lower(groups),table_define,lower(minus_keys)
                        from at_tc_dbcmp_tabsinfo 
                        where lower(tabname) = '%s' 
                              and casetype = '%s' and tc_id = '%s' ''' % (self.user_table_name, str(casetype), caseid)
        prints('generatebydb:', sql)
        lst, msg = thedbobj.execute(sql)
        if len(lst) == 0:
            sql = '''select lower(keys),lower(diff_recs_detail),lower(diff_recs_sum),lower(sum_keys),
                            lower(cnt_keys),lower(rate_keys),lower(wherecond),lower(groups),table_define,lower(minus_keys)
                            from at_tc_dbcmp_tabsinfo 
                            where lower(tabname) = '%s'
                                  and tc_id = '-1' and casetype = '%s' ''' % (self.user_table_name, str(casetype))
            prints('generatebydb:', sql)
            lst, msg = thedbobj.execute(sql)
            if len(lst) == 0:
                sql = '''select lower(keys),lower(diff_recs_detail),lower(diff_recs_sum),lower(sum_keys),
                                lower(cnt_keys),lower(rate_keys),lower(wherecond),lower(groups),table_define,lower(minus_keys)
                                from at_tc_dbcmp_tabsinfo 
                                where lower(tabname) = '%s'
                                      and tc_id = '-1' and casetype = '%s' ''' % (
                self.user_table_name, str(CASE_TYPE_ALL))
                prints('generatebydb:', sql)
                lst, msg = thedbobj.execute(sql)
                if len(lst) == 0:
                    return lst, msg
        return lst, msg

    @staticmethod
    def mvlastsufix(tablename, maxsuflen=1):
        # if tablename like tablename_regioncode_a,then delete _a
        idx = tablename.rfind('_')
        if idx == -1:
            return tablename
        if idx < len(tablename) - maxsuflen - 1:
            return tablename
        return tablename[:idx]

    @staticmethod
    def getvalexceptnone(val):
        if val == None:
            return ''
        return val

    def generatedbdefobj(self, casetype, caseid):
        thedbobj = self.dbObj
        # at_tc_dbcmp_tabsinfo:tablename, casetype(-1:alltype), keys, diff_recs_detail,
        #     diff_recs_sum,sum_keys, cnt_keys, rate_keys, wherecond, groups
        lst, msg = self.gettaballdef(casetype, caseid)
        if len(lst) == 0:
            return None
        self.initbylst(lst)
        return self

    def generatedeflst(self):  # table_define ,use alias if has alias,analyze all var
        table_deflst = []
        tdeflst = []
        # table_define=themap[roottablename].table_define
        if self.table_define != '':
            tdeflst = TimeObj.replace_vars_in_tabname(self.table_define)
            for tdef in tdeflst:
                t = tdef
                t1 = TabClustDef.find_tablename_in_tabdef_sql(tdef, roottablename)
                if t1 != '':
                    t = t1
                table_deflst.append(t)
            prints('generatetabsinfobydb:tabdef:%s,deflst:%s' % (self.table_define, str(table_deflst)))
        return table_deflst

    def redefine_table_define(self):
        if self.table_def_withflag == '':
            if self.table_define.strip().find('select ') < 0 and self.table_define.strip().find(
                    ' where ') < 0 and self.table_define.strip().find(' from ') < 0:
                self.table_define = "(select * from " + self.table_define + ") aa"
            self.table_def_withflag = SqlAnalyze.redefine_sql_table(self.table_define)
        return

    def getparrelstr(self, parreltab='', n=0):
        if n == 0:
            n = self.parrelnum
        if parreltab == '':
            return "/*+ parallel (%s) */" % (n)
        return "/*+ parallel (%s,%s)  */" % (parreltab, n)

    def gettabinfoinenv(self, left, right):
        tabinfo = self.table_def_withflag[left:right + len(' ENV_END')]
        tabinfo2 = self.table_def_withflag[left + len('ENV_BEGIN '):right]
        pt = tabinfo2.strip().split('.')
        dbuser = tab = ''
        if len(pt) == 1:
            dbuser = ''
            tab = pt[0]
        elif len(pt) == 2:
            dbuser = pt[0]
            tab = pt[1]
        return dbuser, tab

    def generate_condstr_forscene(self, wheres, region_code, with_region_code=False):
        cond_map = {}
        sql = "select a.DIFF_SCENE,a.SCENE_COND,a.SCENE_COND_CONST from at_tc_dbcmp_diff_scene_def a,at_tc_dbcmp_diff_scene_sel b " \
              "where a.busi_id = b.busi_id and a.diff_scene = b.diff_scene and b.task_id = '%s' and b.tc_id = '%s' and b.step_id = '%s' and " \
              " b.busi_id = (select distinct busi_id from at_tc_busi_compared_tables c where c.table_name = '%s') and b.flag  = 1  and lower(a.diff_scene_table) = '%s'"
        sql = sql % (
        self.tnc.taskId, self.tnc.caseId, self.tnc.stepId, self.user_table_name, self.user_table_name.lower())
        lst, msg = self.dbObj.execute(sql)
        if len(lst) == 0:
            sql = "select a.DIFF_SCENE,a.SCENE_COND,a.SCENE_COND_CONST from at_tc_dbcmp_diff_scene_def a,at_tc_dbcmp_diff_scene_sel b " \
                  "where a.busi_id = b.busi_id and a.diff_scene = b.diff_scene and b.task_id = '%s' and b.tc_id = '%s' and nvl(b.step_id,'-1') = '-1' and " \
                  " b.busi_id = (select distinct busi_id from at_tc_busi_compared_tables c where c.table_name = '%s') and b.flag  = 1 and lower(a.diff_scene_table) = '%s'"
            sql = sql % (self.tnc.taskId, self.tnc.caseId, self.user_table_name, self.user_table_name.lower())
            lst, msg = self.dbObj.execute(sql)

        for item in lst:
            diffSceneId = item[0]
            SceneCond = item[1]
            if wheres != '':
                SceneCond += ' and ' + wheres
            SceneCondConst = item[2]
            cond_map[diffSceneId] = [SceneCond, SceneCondConst]
        if cond_map == {}:
            cond_map = {'-1': ['', '']}
        return cond_map

    def generate_condstr_fordifftype(self, wheres, region_code, with_region_code=False):
        sql = "select a.diff_type,a.diff_cond from at_tc_dbcmp_diff_type_def a where a.busi_id = (select distinct busi_id from at_tc_busi_compared_tables c where c.table_name = '%s') and lower(diff_table_name) = '%s'"
        sql = sql % (self.user_table_name, self.user_table_name.lower())
        lst, msg = self.dbObj.execute(sql)
        if len(lst) == 0:
            sql = "select a.diff_type,a.diff_cond from at_tc_dbcmp_diff_type_def a where nvl(a.busi_id,-1) = '-1' and lower(diff_table_name) = '%s'  " % (
            self.user_table_name.lower())
            # sql = sql %(self.user_table_name)
            lst, msg = self.dbObj.execute(sql)
        cond_map = {}
        for item in lst:
            difftypeId = item[0]
            diffCond = item[1]
            if wheres != '':
                diffCond += ' and ' + wheres
            cond_map[difftypeId] = diffCond
        if cond_map == {}:
            cond_map['-1'] = ''
        return cond_map

    def isignoredtable(self, tablename, busi_id, dbuser):
        if dbuser.strip() != '' and dbuser.lower() != 'ud' and str(busi_id) != '-1' and tablename.lower().find(
                TabClustDef.BUSI_PREFIX_MAP[str(busi_id)]) == -1:
            return True
        return False

    def findtables(self):
        # table_define = self.table_define
        lefts = Selector.getindexes(self.table_def_withflag, 'ENV_BEGIN ')
        rights = Selector.getindexes(self.table_def_withflag, ' ENV_END')
        tablst = []
        if len(lefts) != len(rights):
            prints('in generate_old_new_tabinfo:sql is error:', self.table_def_withflag)
            return False, {}
        for i in range(0, len(lefts)):
            dbuser, tab = self.gettabinfoinenv(lefts[i], rights[i])
            if self.isignoredtable(tab, self.busi_id, dbuser):
                continue
            tablst.append((dbuser, tab))
        return tablst

    def get_really_unioned_tables(self, normal_table_name):
        flaglst = ['$recentDay', '$recentWeek', '$recentMonth']
        for flag in flaglst:
            if normal_table_name[-1].find(flag):
                break
        replacedstr = ''
        days = []
        if flag == '$recentDay':
            days = self.tnc.getprobablydays()
        if flag == '$recentWeek':
            days = self.tnc.getprobablyweeks()
        if flag == '$recentMonth':
            days = self.tnc.getprobablymonths()

        i = 0
        for it in days:
            dbuserstr = ''
            if normal_table_name[0] != '':
                dbuserstr = normal_table_name[0] + '.'
            replacedstr += '(select * from %s%s )' % (dbuserstr, normal_table_name[-1].replace(flag, str(it)))
            replacedstr += ' union all '
        if replacedstr != '':
            replacedstr = replacedstr[:-len(' union all ')]
        if i > 1:
            replacedstr = '(' + replacedstr + ')'
        return replacedstr

    def get_normal_tables(self):
        if self.normal_table_names != []:
            return self.normal_table_names
        self.normal_table_names = self.findtables()
        for normal_table_name in self.normal_table_names:
            if normal_table_name not in self.really_unioned_tables.keys():
                self.really_unioned_tables[normal_table_name] = self.get_really_unioned_tables(normal_table_name)
        return self.normal_table_names

    def get_ori_tables(self):
        if self.ori_table_names != []:
            return self.ori_table_names
        self.get_normal_tables()
        self.def_normal_ori_map = {}
        dayrange = self.tnc.getprobablydays()
        for dbuser, tab in self.normal_table_names:

            oritabobj = OriTableDef(tab, dbuser, dayrange)
            self.ori_table_names.append(oritabobj)
            if dbuser in self.def_normal_ori_map.keys():
                self.def_normal_ori_map[dbuser] = {}
                if tab not in self.def_normal_ori_map[dbuser].keys():
                    self.def_normal_ori_map[dbuser][tab] = oritabobj.ori_table_lst

        return self.ori_table_names

    def get_all_var_vals(self):
        var_vals = {}
        self.get_ori_tables()
        for ori_tab_obj in self.ori_table_names:
            var_vals_sub = ori_tab_obj.get_all_var_vals()
            for var_name in var_vals_sub.keys():
                if var_name not in var_vals.keys():
                    var_vals[var_name] = []
                for var_val in var_vals_sub[var_name]:
                    if var_val not in var_vals[var_name]:
                        var_vals[var_name].append(var_val)
        return var_vals

    def get_ori_tables_map(self):
        lst_tmp = []
        lst = []
        var_vals = self.get_all_var_vals()
        for ori_tab_obj in self.ori_table_names:
            lst_tmp = []
            if lst == []:
                for it in ori_tab_obj.ori_table_lst:
                    m = {}
                    if ori_tab_obj.table_name not in m.keys():
                        m[ori_tab_obj.table_name] = []
                    m[ori_tab_obj.table_name] = it
                    lst_tmp.append([m])

            else:
                for it in ori_tab_obj.ori_table_lst:
                    for sublst in lst:
                        issame = True
                        for sit in sublst:
                            for k in sit.keys():
                                if not (sit[k].hassameflagval(it) and it.hassameflagval(sit[k])):
                                    issame = False
                                    break
                            if not issame:
                                break
                        if issame:
                            sublst.append({ori_tab_obj.table_name: it})
                        if sublst not in lst_tmp:
                            lst_tmp.append(sublst)

            lst = lst_tmp
        lst = lst_tmp
        return lst

    def get_ori_tables_map2(self):
        lst_tmp = []
        lst = []
        var_vals = self.get_all_var_vals()
        for ori_tab_obj in self.ori_table_names:
            lst_tmp = []
            ori_tab_ununion_lst = ori_tab_obj.get_same_union_lst()
            if lst == []:
                for it in ori_tab_ununion_lst:
                    m = {}
                    if ori_tab_obj.table_name not in m.keys():
                        m[ori_tab_obj.table_name] = []
                    m[ori_tab_obj.table_name] = it
                    lst_tmp.append([m])

            else:
                for it in ori_tab_ununion_lst:
                    for sublst in lst:
                        issame = True
                        for sit in sublst:
                            for k in sit.keys():
                                if not (
                                    sit[k][0].has_same_ununion_flagmap(it[0], False) and it[0].has_same_ununion_flagmap(
                                        sit[k][0], False)):
                                    issame = False
                                    break
                            if not issame:
                                break
                        if issame:
                            sublst.append({ori_tab_obj.table_name: it})
                        if sublst not in lst_tmp:
                            lst_tmp.append(sublst)

            lst = lst_tmp
        lst = lst_tmp
        return lst

    def get_last_table_define_3(self):
        if self.last_table_define_pair_map != {}:
            return self.last_table_define_pair_map

        lst = self.get_ori_tables_map2()
        table_defines = []
        last_table_define_pair_map = {}

        for slst in lst:
            define_old = self.table_def_withflag
            define_new = self.table_def_withflag
            region_code = ''
            table_with_region = []
            for it in slst:
                for k in it.keys():
                    unionobj = it[k][0]
                    if '$regionCodeEach' in unionobj.flagmap.keys() and region_code == '':
                        region_code = unionobj.flagmap['$regionCodeEach']
                    if '$regionCodeEach' in unionobj.flagmap.keys():
                        if it not in table_with_region:
                            table_with_region.append(it)
                            break

            for it in slst:
                for k in it.keys():
                    dbuser = ''
                    for ori_tab_obj in self.ori_table_names:
                        if ori_tab_obj.table_name == k:
                            dbuser = ori_tab_obj.dbuser
                            break
                    real_table_name_lst = []
                    otablst = []
                    ntablst = []
                    for i in range(0, len(it[k])):
                        if not it[k][i].has_same_region_code(region_code):
                            continue
                        real_table_name = it[k][i].generaterealtablename()
                        otab = self.tnc.gettablename4old(real_table_name, dbuser)
                        ntab = self.tnc.gettablename4new(real_table_name, dbuser)
                        real_table_name_lst.append(real_table_name)
                        otablst.append(otab)
                        ntablst.append(ntab)
                    union_str_old = union_str_new = ''
                    j = 0
                    for i in range(0, len(it[k])):
                        if not it[k][i].has_same_region_code(region_code):
                            continue
                        union_str_old += '(select * from EVN_BEGIN %s ENV_END ) union all ' % (otablst[j])
                        union_str_new += '(select * from ENV_BEGIN %s ENV_END ) union all ' % (ntablst[j])
                        j += 1
                    if union_str_old != '':
                        union_str_old = union_str_old[:-len(' union all ')]
                    if union_str_new != '':
                        union_str_new = union_str_new[:-len(' union all ')]
                    if len(it[k]) > 1:
                        union_str_old = ' (' + union_str_old + ') '
                        union_str_new = ' (' + union_str_new + ') '
                    define_old = define_old.replace('ENV_BEGIN ' + k + ' ENV_END', union_str_old)
                    define_new = define_new.replace('ENV_BEGIN ' + k + ' ENV_END', union_str_new)
            for it in slst:
                for k in it.keys():
                    for tobj in it[k]:
                        if not tobj.has_same_region_code(region_code):
                            continue
                        for r in tobj.flagmap.keys():
                            key = r
                            val = tobj.flagmap[r]
                            define_old = define_old.replace(key, str(val))
                            define_new = define_new.replace(key, str(val))
                        break
            if region_code == '':
                region_code = '-1'
            if region_code not in last_table_define_pair_map.keys():
                last_table_define_pair_map[region_code] = []
            if [define_old, define_new] not in last_table_define_pair_map[region_code]:
                last_table_define_pair_map[region_code].append([define_old, define_new])
        self.last_table_define_pair_map = last_table_define_pair_map

        return self.last_table_define_pair_map

    def get_last_ori_tables(self, taskId, caseId):
        last_ori_tables = {}
        for oritabobj in self.ori_table_names:
            region_code_idx = oritabobj.region_idx
            tablst = oritabobj.ori_table_lst
            dbuser = oritabobj.dbuser
            the_table_name = oritabobj.table_name
            if dbuser.strip() != '':
                the_table_name = dbuser + '.' + oritabobj.table_name
            root_table_name = oritabobj.table_name[:region_code_idx - 1]
            last_ori_tables[root_table_name] = {}
            for tabobj in tablst:
                table_name = tabobj.generaterealtablename()
                region_code = StringObj.findnext(table_name, region_code_idx, '_')

                old_tab = self.tnc.gettablename4old(table_name, dbuser)
                new_tab = self.tnc.gettablename4new(table_name, dbuser)
                last_ori_tables[root_table_name][region_code] = (table_name, dbuser, old_tab, new_tab)
        return last_ori_tables

    def get_last_table_define(self):

        return self.get_last_table_define_3()

    def make_drop_table_sql(self, conditions=''):
        sqlsmap = {}
        self.get_ori_tables()
        if self.copyoritableflag:
            for ori_table_obj in self.ori_table_names:
                tablst = ori_table_obj.ori_table_lst
                dbuser = ori_table_obj.dbuser
                for tabobj in tablst:
                    tabobj1 = tabobj.generatenewobjwithconditions(conditions)
                    table_name = tabobj1.generaterealtablename()
                    old_tab = self.tnc.gettablename4old(table_name, dbuser)
                    new_tab = self.tnc.gettablename4new(table_name, dbuser)

                    sql1 = "truncate table %s;\ndrop table %s" % (old_tab, old_tab)
                    sql2 = "truncate table %s;\ndrop table %s" % (new_tab, new_tab)
                    sqlsmap[table_name] = (sql1, sql2)
        else:
            # last_ori_tables = self.get_last_ori_tables(self.tnc.taskId,self.tnc.caseId)
            # last_table_define_pair_lst = self.get_last_table_define(last_ori_tables)
            # for last_table_define_pair in last_table_define_pair_lst:
            #    last_table_define_old,last_table_define_new = last_table_define_pair

            # sql1 = "drop table %s;" %(oldtable_name)
            # sql2 = "drop table %s;" %(newtable_name)
            # sql1 = SqlTableConvertor(sql1,self.tnc)
            # sql2 = SqlTableConvertor(sql2,self.tnc)
            # sqlsmap[table_name] = (sql1,sql2)
            pass
        return sqlsmap

    def make_rename_oritable_sql(self):
        # oracle only rename tables in same inst and same dbuser,so if old_tab.dbuser != dbuser,should use create table sql
        # if ori_table_obj.dbuser is null, default value is dbuser of the dblink to bossenv
        sqls = {}
        self.get_ori_tables()
        if self.copyoritableflag:
            for ori_table_obj in self.ori_table_names:
                tablst = ori_table_obj.ori_table_lst
                dbuser = ori_table_obj.dbuser
                for tabobj in tablst:
                    table_name = tabobj.generaterealtablename()
                    old_tab = self.tnc.gettablename4old(table_name, dbuser)
                    new_tab = self.tnc.gettablename4new(table_name, dbuser)
                    if dbuser.strip() == '':
                        old_tab_name = old_tab.split('.')[-1]
                        new_tab_name = new_tab.split('.')[-1]
                        sql1 = sql2 = ''
                        if table_name != old_tab_name:
                            sql1 = "rename %s to %s;\ncreate table %s as select * from %s where rownum < 1;" % (
                            table_name, old_tab_name, table_name, old_tab_name)
                        if table_name != new_tab_name:
                            sql2 = "rename %s to %s;\ncreate table %s as select * from %s where rownum < 1;" % (
                            table_name, new_tab_name, table_name, new_tab_name)
                    else:
                        old_dbuser = old_tab.split('.')[0]
                        new_dbuser = new_tab.split('.')[0]
                        sql1 = sql2 = ''
                        old_tab_name = old_tab.split('.')[-1]
                        new_tab_name = new_tab.split('.')[-1]
                        if dbuser != old_dbuser:
                            sql1 = "create table %s as select * from %s.%s;\ntruncate table %s.%s;\ntruncate table %s.%s;" % (
                            old_tab, dbuser, table_name, dbuser, table_name, dbuser, table_name)
                        else:
                            if old_tab_name != table_name:
                                sql1 = "rename %s to %s;\ncreate table %s as select * from %s where rownum < 1;" % (
                                table_name, old_tab_name, table_name, old_tab_name)
                        if dbuser != new_dbuser:
                            sql2 = "create table %s as select * from %s.%s;\ntruncate table %s.%s;\ntruncate table %s.%s;" % (
                            new_tab, dbuser, table_name, dbuser, table_name, dbuser, table_name)
                        else:
                            if new_tab_name != table_name:
                                sql2 = "rename %s to %s;\ncreate table %s as select * from %s where rownum < 1;" % (
                                table_name, new_tab_name, table_name, new_tab_name)
                    if sql1 == sql2 and sql1 == '':
                        continue
                    sqls[table_name] = (sql1, sql2)
        else:
            return True, {}
        return True, sqls

    def gettabcolumns(self, table_name, dblnkstr=''):
        dbobj = self.dbObj
        lnkstr = ''
        if dblnkstr != '':
            lnkstr = '@' + dblnkstr
        sql = "select column_name from all_tab_cols%s where lower(table_name) ='%s' " % (
        lnkstr, table_name.split('.')[-1].split()[0].lower())
        lst, msg = dbobj.execute(sql)
        lst0 = []
        strfields = ''
        for it in lst:
            lst0.append(it[0])
            strfields += it[0] + ','
        if strfields != '':
            strfields = strfields[:-1]

        return lst0, strfields

    def getunionedtabcolumns(self, tabsql, dblnkstr):
        if tabsql.find(' union ') > -1:
            thetabsql = tabsql[:tabsql.find(' union ')]

            return self.getunionedtabcolumns(thetabsql, dblnkstr)
        sa = SqlAnalyze(tabsql)
        fm = sa.getfields()
        isselectall = False

        for k in fm.keys():
            if k.find('*') > -1:
                isselectall = True
                break
        if not isselectall:
            strfields = ''
            for k in fm.keys():
                strfields += fm[k] + ','
            if strfields != '':
                strfields = strfields[:-1]
            return tabsql, strfields

        thecols = []
        thecolswithtab = []
        thecolsmap = {}
        tm = sa.gettablenames()
        taliasmap = {}
        for k in tm.keys():
            k1 = k.replace('ENV_BEGIN ', '')
            k1 = k1.replace(' ENV_END', '')
            alias = k1.strip().split(' ')
            taliasmap[alias[-1]] = alias[0]
        for k in fm.keys():
            idx = k.find('.*')
            idx1 = k.find('*')
            if idx1 > -1:
                theidx = idx1
                r1 = k[:idx1].split('.')[-1]
                if idx > -1:
                    theidx = idx
                    r1 = k[:idx].split('.')[-1]
                for r in taliasmap.keys():
                    if idx > -1 and r.lower().strip() != r1.lower().strip():
                        continue
                    cols, strcols_tmp = self.gettabcolumns(taliasmap[r], dblnkstr)
                    if r not in thecolsmap.keys():
                        thecolsmap[r] = []
                    for it in cols:
                        if it.lower() not in thecols:
                            thecols.append(it.lower())
                            thecolswithtab.append(k[:theidx + 1] + it.lower())
                            thecolsmap[r].append(k[:theidx + 1] + it.lower())
            else:
                if 'XXXX' not in thecolsmap.keys():
                    thecolsmap['XXXX'] = []
                falias = k.split(' ')
                if falias[0].lower() not in thecols:
                    thecols.append(falias[0].lower())
                    thecolswithtab.append(k)
                    thecolsmap['XXXX'].append(k)
        strfields = ''
        for k in fm.keys():
            idx = k.find('.*')
            idx1 = k.find('*')
            theidx = idx1
            if idx > -1:
                theidx = idx
            if theidx > -1 and (k[:theidx] in thecolsmap.keys() and k[:theidx].strip() != ''):
                fm[k] = StringObj.convertlst2str(thecolsmap[k[:theidx]])
                if fm[k] != '':
                    strfields += ',' + fm[k]
            elif theidx > -1 and k[:theidx].strip() == '':
                for r in thecolsmap.keys():
                    strtmp = StringObj.convertlst2str(thecolsmap[k[:theidx]])
                    if strtmp != '':
                        fm[k] += ',' + strtmp
                strfields += fm[k]
            else:
                fm[k] = k
                if k != '':
                    strfields += ',' + k

        if strfields != '':
            strfields = strfields.strip(',')
        tabdefines = ''
        for k in tm.keys():
            if tm[k] == '':
                tabdefines += ',' + k
            else:
                if tm[k].lstrip()[0] != '(':
                    tabdefines += ',(' + tm[k] + ') '
                else:
                    tabdefines += ',' + tm[k]

                def isnormalalias(k):
                    idx1 = k.find('_')
                    if idx1 > -1:
                        idx2 = k[idx1 + 1:].find('_')
                        if idx2 > -1:
                            idx2 += idx1 + 1
                            if k[:idx1 + 1] == 't_' and k[idx1 + 1:idx2].isdigit() and k[idx2 + 1:].isdigit():
                                return True
                    return False

                if not isnormalalias(k):
                    tabdefines += ' ' + k
        if tabdefines != '':
            tabdefines = tabdefines[1:]
        ordermap = sa.getotherfields('order')
        groupmap = sa.getotherfields('group')
        havingmap = sa.getotherfields('having')
        ordstr = ''
        groupstr = ''
        havingstr = ''
        for k in ordermap.keys():
            ordstr += ',' + k
        for k in groupmap.keys():
            groupstr += ',' + k
        for k in havingmap.keys():
            havingstr += ',' + k
        if ordstr != '':
            ordstr = ' order by ' + ordstr[1:]
        if groupstr != '':
            groupstr = ' group by ' + groupstr[1:]
        if havingstr != '':
            havingstr = ' having ' + havingstr[1:]
        strsql = "select " + strfields + ' from ' + tabdefines + ' where ' + sa.selector.cond.generatecond() \
                 + ordstr + groupstr + havingstr

        return strsql, strfields

    def get_really_used_cols(self, table_name_sqls, key_diff_cols):
        key_diff_cols_intab = []
        key_diff_cols_used = ''
        table_cols = ''
        connstr = ''
        dblnk = TestDBEnvManager.getTestDBEnvManager(self.dbObj, self.tnc.projId).dblnk1.dblnk

        if table_name_sqls.find('select ') == -1 and table_name_sqls.find(',') == -1:
            table_col_lst, table_cols = self.gettabcolumns(table_name_sqls, dblnk)
        else:
            table_defines, table_cols = self.getunionedtabcolumns(table_name_sqls, dblnk)
        for it in key_diff_cols.split(','):
            colname = it.split('.')[-1].split()[0]
            if colname in table_cols.split(','):
                key_diff_cols_intab.append(colname)
        if key_diff_cols_intab == []:
            key_diff_cols_intab = key_diff_cols.split(',')
        for it in key_diff_cols_intab:
            key_diff_cols_used += it.split('.')[-1].split()[0] + ','
        if key_diff_cols_used != '':
            key_diff_cols_used = key_diff_cols_used[:-1]
        if key_diff_cols_used == '':
            key_diff_cols_used = key_diff_cols

        return key_diff_cols_used

    def get_root_table_name_from_normal_table(self, table_name):
        idx = table_name.find('_$')
        if idx > -1:
            return table_name[:idx]
        return table_name

    def make_delete_result_sql(self):
        sqlsmap = {}
        sql0 = "delete from at_tc_dbcmp_diff_detail where task_id  = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id  = '%s'"
        sql1 = "delete from at_tc_dbcmp_diff_stat where task_id  = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id = '%s'"
        sql2 = "delete from at_tc_dbcmp_diff_stat1 where task_id  = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id = '%s'"
        sql3 = "delete from at_tc_dbcmp_diff_count where task_id  = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id = '%s'"
        self.get_ori_tables()
        rootTcId = self.getroottc(self.tnc.caseId, self.dbObj)
        table_def_pair_map = self.get_last_table_define()
        for region_code in table_def_pair_map.keys():
            table_name = self.user_table_name  # +'_'+str(region_code) self.concat_tablename_regioncode(self.user_table_name,region_code)
            root_table_name = table_name
            if table_name not in sqlsmap.keys():
                sql = sql0 % (self.tnc.taskId, rootTcId, root_table_name, self.tnc.caseId)
                sql += ';\n'
                sql += sql1 % (self.tnc.taskId, rootTcId, root_table_name, self.tnc.caseId)
                sql += ';\n'
                sql += sql2 % (self.tnc.taskId, rootTcId, root_table_name, self.tnc.caseId)
                sql += ';\n'
                sql += sql3 % (self.tnc.taskId, rootTcId, root_table_name, self.tnc.caseId)
                sqlsmap[root_table_name] = sql
            break

        return sqlsmap

    def make_update_result_sql(self):
        sqlsmap = {}
        sql0 = "update at_tc_dbcmp_diff_detail set table_name = '%s' where task_id = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id = '%s'"
        sql1 = "update at_tc_dbcmp_diff_stat set table_name = '%s' where task_id = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id = '%s'"
        sql2 = "update at_tc_dbcmp_diff_stat1 set table_name = '%s' where task_id = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id = '%s'"
        self.get_ori_tables()
        rootTcId = self.getroottc(self.tnc.caseId, self.dbObj)
        table_def_pair_map = self.get_last_table_define()
        for region_code in table_def_pair_map.keys():
            table_name = self.concat_tablename_regioncode(self.user_table_name, region_code)
            root_table_name = table_name[:table_name.rfind('_')]
            sql = sql0 % (root_table_name, self.tnc.taskId, rootTcId, table_name, self.tnc.caseId)
            if region_code != '':
                sql += " and region_code = '%s' " % (str(region_code))
            sql += ";\n"
            sql += sql1 % (root_table_name, self.tnc.taskId, rootTcId, table_name, self.tnc.caseId)
            if region_code != '':
                sql += " and region_code = '%s' " % (str(region_code))
            sql += ";\n"
            sql += sql2 % (root_table_name, self.tnc.taskId, rootTcId, table_name, self.tnc.caseId)
            if region_code != '':
                sql += " and region_code = '%s' " % (str(region_code))
            if root_table_name not in sqlsmap.keys():
                sqlsmap[root_table_name] = {}
            if region_code == '':
                sqlsmap[root_table_name]['all'] = sql
            else:
                sqlsmap[root_table_name][region_code] = sql

        return sqlsmap

    def make_move_result_sql(self, thedblnk):
        sqlsmap = {}
        sql0 = "insert into at_tc_dbcmp_diff_detail select %s * from at_tc_dbcmp_diff_detail%s where task_id  = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id = '%s'"
        sql1 = "insert into at_tc_dbcmp_diff_stat select %s * from at_tc_dbcmp_diff_stat%s where task_id  = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id = '%s'"
        sql2 = "insert into at_tc_dbcmp_diff_stat1 select %s * from at_tc_dbcmp_diff_stat%s where task_id  = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id = '%s'"
        sql3 = "insert into at_tc_dbcmp_diff_count select %s * from at_tc_dbcmp_diff_count%s where task_id  = '%s' and tc_id = '%s' and table_name = '%s' and sub_tc_id = '%s'"
        rootTcId = self.getroottc(self.tnc.caseId, self.dbObj)
        table_name = self.user_table_name
        sql = sql0 % (self.getparrelstr(), thedblnk, self.tnc.taskId, rootTcId, table_name, self.tnc.caseId)
        sql += ';\n'
        sql += sql1 % (self.getparrelstr(), thedblnk, self.tnc.taskId, rootTcId, table_name, self.tnc.caseId)
        sql += ';\n'
        sql += sql2 % (self.getparrelstr(), thedblnk, self.tnc.taskId, rootTcId, table_name, self.tnc.caseId)
        sql += ';\n'
        sql += sql3 % (self.getparrelstr(), thedblnk, self.tnc.taskId, rootTcId, table_name, self.tnc.caseId)
        sqlsmap[table_name] = sql

        return sqlsmap

    def get_ignore_errorcodes(self):
        errorcodes = []
        sql = "select err_code from at_tc_ignore_errcode where busi_id = '%s' and tc_id = '%s' and table_name = '%s' " % (
        self.busi_id, self.tnc.caseId, self.user_table_name)
        lst, msg = self.dbObj.execute(sql)
        if len(lst) == 0:
            sql = "select err_code from at_tc_ignore_errcode where busi_id = '%s' and tc_id = '-1' and table_name = '%s' " % (
            self.busi_id, self.user_table_name)
            lst, msg = self.dbObj.execute(sql)
            if len(lst) == 0:
                sql = "select err_code from at_tc_ignore_errcode where busi_id = '-1' and tc_id = '-1' and table_name = '%s'" % (
                self.user_table_name)
                lst, msg = self.dbObj.execute(sql)
                if len(lst) == 0:
                    return []
        for it in lst:
            errorcodes.append(it[0])
        return errorcodes

    def make_count_each_tabledef_sql(self, thedblnk):
        count_sql_map = {}
        count_sql_map[self.user_table_name] = {}
        self.get_ori_tables()
        table_def_pair_map = self.get_last_table_define()
        errorcodes = self.get_ignore_errorcodes()
        countstr = ''
        for it in self.cntkeys.split(','):
            it1 = it.split('.')[-1].split()[0]
            if it1 != '1' and it1 != '*':
                it1 = 'distint ' + it1
            countstr += "'count(" + it1 + ") = '||count(" + it1 + ")?"
        if countstr != '':
            countstr = countstr[:-1]
        rootTcId = self.getroottc(self.tnc.caseId, self.dbObj)

        for region_code in table_def_pair_map.keys():
            table_def1, table_def2, real_table1, real_table2 = self.getRealTableDef(
                table_def_pair_map[region_code][0][0], region_code, '', thedblnk)

            count_sql_map[self.user_table_name][str(region_code)] = {}
            scenecondmap = self.generate_condstr_forscene('', region_code)
            if scenecondmap == {}:
                scenecondmap = {'-1': ['', '']}
            dtcondmap = self.generate_condstr_fordifftype('', region_code)
            if dtcondmap == {}:
                dtcondmap = {'-1': ''}
            diff_scene_ids = self.diff_scene_ids
            if diff_scene_ids == []:
                diff_scene_ids = ['']
            for sceneId in diff_scene_ids:
                if sceneId not in scenecondmap.keys() and sceneId != '':
                    continue
                cond = ''
                if sceneId != '' or sceneId in scenecondmap.keys():
                    cond0 = scenecondmap[sceneId][0]
                    cond1 = scenecondmap[sceneId][1]
                    if cond0 != '' and cond1 != '':
                        cond = cond0 + ' and (' + cond1 + ')'
                    elif cond1 == '':
                        cond = cond0
                    else:
                        cond = cond1
                if cond != '':
                    cond = ' where ' + cond
                for difftype in dtcondmap.keys():
                    dcond = dtcondmap[difftype]
                    condall = ''
                    dcond = dcond.replace(' ax ', '( ' + table_def1 + ') ax ').replace(' ay ',
                                                                                       '(' + table_def2 + ') ay ')
                    if cond != '' and dcond != '':
                        condall = cond + ' and (' + dcond + ' ) '
                    elif dcond != '':
                        condall = ' where (' + dcond + ')'
                    elif cond != '':
                        condall = cond
                    else:
                        condall = ''
                    sql1 = "select %s %s from (%s) %s" % (self.getparrelstr(), countstr, table_def1, condall)
                    sql2 = "select %s %s from (%s) %s" % (self.getparrelstr(), countstr, table_def2, condall)
                    condall2 = condall
                    sql3 = sql4 = "''"
                    if errorcodes != []:
                        errors = "'" + "','".join(errorcodes) + "'"
                        if condall2.strip() == '':
                            condall2 = " where err_code in (%s)" % (errors)
                        else:
                            condall2 += " and err_code in (%s)" % (errors)
                        sql3 = "select %s %s from (%s) %s" % (self.getparrelstr(), countstr, table_def1, condall2)
                        sql4 = "select %s %s from (%s) %s" % (self.getparrelstr(), countstr, table_def2, condall2)
                    if sceneId not in count_sql_map[self.user_table_name][str(region_code)].keys():
                        count_sql_map[self.user_table_name][str(region_code)][sceneId] = {}
                    # sql = "delete from at_tc_dbcmp_diff_count where task_id  = %s and tc_id = %s and table_name = '%s' and diff_scene = '%s' and diff_type = '%s' and SUB_TC_ID = '%s';\n" %(self.tnc.taskId,rootTcId,self.user_table_name,sceneId,difftype,self.tnc.caseId)
                    sql = "insert into at_tc_dbcmp_diff_count (TASK_ID,TC_ID,TABLE_NAME,TYPE,OLD_INFO,NEW_INFO,OLD_IGNORE_INFO,NEW_IGNORE_INFO,SUB_TC_ID,DIFF_TYPE,DIFF_SCENE,REGION_CODE) select '%s','%s','%s','-1',(%s) OLD_INFO,(%s) NEW_INFO,(%s) OLD_IGNORE_INFO,(%s) NEW_IGNORE_INFO,'%s','%s','%s','%s' from dual;" % (
                    self.tnc.taskId, rootTcId, self.user_table_name, sql1, sql2, sql3, sql4, self.tnc.caseId, difftype,
                    sceneId, region_code)

                    count_sql_map[self.user_table_name][str(region_code)][sceneId][difftype] = sql  # (sql1,sql2)
        return count_sql_map

    def make_sum_each_tabledef_sql(self, thedblnk):
        sum_sql_map = {}
        self.get_ori_tables()
        table_def_pair_map = self.get_last_table_define()
        sumstr = ''
        for it in self.sumkeys.split(','):
            sumstr += "'sum(" + it + ") = '||sum(" + it + ")?"
        if sumstr != '':
            sumstr = sumstr[:-1]
        errorcodes = self.get_ignore_errorcodes()
        rootTcId = self.getroottc(self.tnc.caseId, self.dbObj)
        sum_sql_map[self.user_table_name] = {}
        for region_code in table_def_pair_map.keys():
            the_user_table_name = self.concat_tablename_regioncode(self.user_table_name, region_code)
            table_def1, table_def2, real_table1, real_table2 = self.getRealTableDef(
                table_def_pair_map[region_code][0][0], region_code, '', thedblnk)
            sum_sql_map[self.user_table_name][str(region_code)] = {}
            scenecondmap = self.generate_condstr_forscene('', region_code)
            if scenecondmap == {}:
                scenecondmap = {'-1': ['', '']}

            dtcondmap = self.generate_condstr_fordifftype('', region_code)
            if dtcondmap == {}:
                dtcondmap = {'-1': ''}
            diff_scene_ids = self.diff_scene_ids
            if diff_scene_ids == []:
                diff_scene_ids = ['']
            for sceneId in diff_scene_ids:
                if sceneId not in scenecondmap.keys() and sceneId != '':
                    continue
                sum_sql_map[self.user_table_name][str(region_code)][sceneId] = {}
                cond = ''
                if sceneId != '' or sceneId in scenecondmap.keys():
                    cond0 = scenecondmap[sceneId][0]
                    cond1 = scenecondmap[sceneId][1]
                    if cond0 != '' and cond1 != '':
                        cond = cond0 + ' and ' + cond1
                    elif cond1 == '':
                        cond = cond0
                    else:
                        cond = cond1
                if cond != '':
                    cond = ' where ' + cond
                for difftype in dtcondmap.keys():
                    dtcond = dtcondmap[difftype]
                    condall = ''
                    if cond != '' and dtcond != '':
                        condall = cond + ' and (' + dtcond + ')'
                    elif cond != '':
                        condall = cond
                    elif dtcond != '':
                        condall = ' where (' + dtcond + ')'
                    else:
                        condall = ''
                    condall2 = condall
                    sql3 = sql4 = "''"
                    if errorcodes != []:
                        errors = "'" + "','".join(errorcodes) + "'"
                        if condall2.strip() == '':
                            condall2 = " where err_code in (%s)" % (errors)
                        else:
                            condall2 += " and err_code  in (%s)" % (errors)
                        sql3 = "select %s %s from (%s) %s" % (self.getparrelstr(), sumstr, table_def1, condall2)
                        sql4 = "select %s %s from (%s) %s" % (self.getparrelstr(), sumstr, table_def2, condall2)
                    sql0 = "select %s %s from (%s) %s" % (self.getparrelstr(), sumstr, table_def1, condall)
                    sql1 = "select %s %s from (%s) %s" % (self.getparrelstr(), sumstr, table_def2, condall)
                    sql = "update  at_tc_dbcmp_diff_count set OLD_SUM_INFO = (%s),NEW_SUM_INFO=(%s),OLD_IGNORE_SUM_INFO=(%s),NEW_IGNORE_SUM_INFO=(%s) where TASK_ID = '%s' and TC_ID = '%s' and TABLE_NAME='%s' and SUB_TC_ID='%s' and DIFF_TYPE='%s' and DIFF_SCENE = '%s' and REGION_CODE='%s';" % (
                    sql0, sql1, sql3, sql4, self.tnc.taskId, rootTcId, self.user_table_name, self.tnc.caseId, difftype,
                    sceneId, region_code)

                    sum_sql_map[self.user_table_name][str(region_code)][sceneId][difftype] = sql  # (sql0,sql1)
        return sum_sql_map

    def getroottc(self, caseId, dbObj):
        if caseId in TabClustDef.TC_ROOT_TC_MAP.keys():
            return TabClustDef.TC_ROOT_TC_MAP[caseId]
        sql0 = "select tc_id,parent_tc_id,root_tc_id from at_tc where tc_id = '%s'" % (caseId)
        lst, msg = dbObj.execute(sql0)
        if len(lst) == 0:
            TabClustDef.TC_ROOT_TC_MAP[caseId] = -1
            return -1
        else:
            TabClustDef.TC_ROOT_TC_MAP[caseId] = lst[0][2]
            return lst[0][2]

    def concat_tablename_regioncode(self, table_name, region_code):
        if str(region_code) == '-1':
            return table_name + '_all'
        else:
            return table_name + '_' + str(region_code)

    def get_regioncode_from_table_name(self, concated_table_name):
        idx = concated_table_name.rfind('_')
        if idx == -1:
            return -1
        region_str = concated_table_name[idx + 1:]
        if region_str == 'all':
            return -1
        return int(region_str)

    def make_tmp_table_names(self, real_table, region_code, recent_days, dblnk_str, old_new_flag, adddblnk=False):
        real_table = self.user_table_name
        if int(region_code) > 0:
            real_table += '_' + str(region_code)
        real_table += '_' + str(recent_days[-1])
        if dblnk_str != '' and dblnk_str[1] != '@':
            dblnk_str = '@' + dblnk_str
        if old_new_flag == 0:
            if not adddblnk:
                return 'dfo_' + real_table
            return 'dfo_' + real_table + dblnk_str
        return 'dfn_' + real_table

    def get_mapped_table(self, table_name, dbObj, task_id, tc_id, current_date, region_code, old_new_flag):
        prints(get_current_func_name4log(), 'table_name:', table_name, 'task_id:', task_id, 'tc_id:', tc_id,
               'current_date:', current_date, 'region_code:', region_code, 'old_new_flag:', old_new_flag)
        idx = -1
        if str(region_code) != '-1' and str(region_code) != '':
            idx = table_name.find(str(region_code))
        curr_tab1 = curr_tab2 = thetablename = ''
        sql = "select curr_table_name from at_tc_table_names_map where task_id = '%s' and tc_id = '%s' and region_code = '%s' and table_name = '%s' and now = '%s' and proj_id = '%s'"
        sql1 = sql % (str(task_id), str(tc_id), str(region_code), table_name, str(current_date), str(self.tnc.projId))
        lst1, msg1 = dbObj.execute(sql1)
        if len(lst1) > 0:
            curr_tab1 = str(lst1[0][0]) + str(env_type)
            return curr_tab1
        if idx > -1:
            thetablename = table_name[:idx]
        else:
            idx = len(table_name)
            if idx > 16:
                idx = 16
            thetablename = table_name[:idx]
        curr_tab1 = thetablename + str(tc_id)[-4:] + str(current_date)[4:]
        if str(region_code) != '-1' and str(region_code) != '':
            curr_tab1 += str(region_code)
        # curr_tab1 = thetablename+str(caseId[-4:])+str(diff_sceneId)+str(daynow)
        sql1 = "insert into at_tc_table_names_map (table_name, task_id,tc_id, region_code,curr_table_name,now) \
                 values('%s','%s','%s','%s','%s','%s')" % (
        table_name, str(task_id), str(tc_id), str(region_code), curr_tab1, str(current_date))
        dbObj.execute(sql1)

        return curr_tab1 + str(old_new_flag)

    def redefine_table_def_sql(self, table_def, dbObj, dblnk_str, task_id, tc_id, region_code, current_date,
                               old_new_flag, adddblnk=False):
        tabdeflst = table_def.split('ENV_BEGIN ')
        tabdef2 = tabdeflst[0]
        old_table = tabdeflst[0]

        if len(tabdeflst) == 1:
            dbuser = ''
            if old_table.find('.') > 0:
                dbuser = old_table.split('.')[0]
            old_table = old_table.split('.')[-1]
            # if old_new_flag == 0:
            #    old_table = self.tnc.gettablename4old(old_table,dbuser)
            # else:
            #    old_table = self.tnc.gettablename4new(old_table,dbuser)
            old_table = self.get_mapped_table(old_table, dbObj, task_id, tc_id, current_date, region_code, old_new_flag)
            if adddblnk:
                old_table += dblnk_str
            return old_table
        i = 1
        while i < len(tabdeflst):
            idx = tabdeflst[i].find(' ENV_END')
            if idx > 0:
                old_table = tabdeflst[i][:idx]
                dbuser = ''
                if old_table.find('.') > 0:
                    dbuser = old_table.split('.')[0]
                old_table = old_table.split('.')[-1]
                # if old_new_flag == 0:
                #    old_table = self.tnc.gettablename4old(old_table,dbuser)
                # else:
                #    old_table = self.tnc.gettablename4new(old_table,dbuser)
                old_table = self.get_mapped_table(old_table, dbObj, task_id, tc_id, current_date, region_code,
                                                  old_new_flag)
                if adddblnk:
                    old_table += dblnk_str
                tabdef2 += tabdeflst[i].replace(tabdeflst[i][:idx + len(' ENV_END')], old_table)
            else:
                return ''
            i += 1
        return tabdef2

    def redefine_table_def_sql2(self, table_def, dbObj, dblnk_str, task_id, tc_id, region_code, recent_dates,
                                old_new_flag):
        sql = ''
        used_date = []
        for current_date in recent_dates:
            if current_date in used_date:
                continue
            used_date.append(current_date)
            sql1 = self.redefine_table_def_sql(table_def, dbObj, dblnk_str, task_id, tc_id, region_code, current_date,
                                               old_new_flag)
            sql += '(' + sql1 + ')' + ' union all '
        if len(sql) > len('union all'):
            sql = sql[:-len(' union all ')]
        return sql

    def getRealUsedFields(self, table_def, used_fields='', dblnk_str=''):
        if used_fields == '':
            sql00, used_fields = self.getunionedtabcolumns(table_def, dblnk_str)

        used_fields1 = ""
        for it in used_fields.split(GlobalVar.key_split_char):
            if it == '':
                continue

            removedkey = SqlItem.removeDottedAlias(it)
            removedkey = removedkey.split()[-1]
            used_fields1 += removedkey.split()[0].lstrip() + GlobalVar.key_split_char
        used_fields1 = used_fields1.rstrip(GlobalVar.key_split_char)
        return used_fields1

    def getOriMinusKeys(self, dblnk_str):
        minus_keys = self.minus_item.recs
        ori_minus_keys = self.minus_item.getIDKeys()
        if minus_keys == '':
            minus_keys = self.key_item.recs + ',' + self.diff_item.recs
            self.minus_item = MinusItem(minus_keys, self.dbObj)
            self.minus_item.settypemap(self.generatekeytypemap(self.minus_item.recs, self.minus_item, dblnk_str))
            ori_minus_keys = self.minus_item.getIDKeys()
        return ori_minus_keys, minus_keys

    def getRealTableDef(self, table_def, region_code, used_fields='', dblnk_str=''):

        recent_dates = self.tnc.getprobablydays()
        if len(recent_dates) == 1:
            recent_dates.append(recent_dates[0])

        real_table1 = self.make_tmp_table_names(self.user_table_name, region_code, recent_dates, dblnk_str, 0)
        real_table2 = self.make_tmp_table_names(self.user_table_name, region_code, recent_dates, dblnk_str, 1)
        table_def1 = self.redefine_table_def_sql2(table_def, self.dbObj, dblnk_str, self.tnc.taskId, self.tnc.caseId,
                                                  region_code, recent_dates, 0)
        table_def2 = self.redefine_table_def_sql2(table_def, self.dbObj, '', self.tnc.taskId, self.tnc.caseId,
                                                  region_code, recent_dates, 1)

        return table_def1, table_def2, real_table1, real_table2

    def isDiffTypeUsedCount(self, diffTypeId, typecond):
        if str(diffTypeId) in ('27',):
            return True
        idx = typecond.find('count4each(')
        idx2 = typecond.find('count4each(', idx + 11)
        if idx > -1 and idx2 > -1:
            return True
        idx = typecond.find('sum4each(')
        idx2 = typecond.find('sum4each(', idx + 11)
        if idx > -1 and idx2 > -1:
            return True
        return False

    def getgroups_counts_extras(self, typecond, ptab, qtab):
        groupstr = countstr = extra_conditions = ''
        newtypecond = typecond
        idx = typecond.find('count4each(')
        idx2 = typecond.find('count4each(', idx + 11)
        operation = 'count'
        if idx < 0 or idx2 < 0:
            idx = typecond.find('sum4each(')
            idx2 = typecond.find('sum4each(', idx + 11)
            operation = 'sum'
            if idx < 0 or idx2 < 0:
                return groupstr, countstr, extra_conditions, newtypecond
        newtypecond = ''
        flag = ['=', '<>', '!=', '<', '>', '<=', '>=']
        idx3 = -1
        flaglen = 0
        theflag = ''
        for it in flag:
            idx3 = typecond.find(it)
            if idx3 > -1:
                flaglen = len(it)
                theflag = it
                break
        firstcount = typecond[:idx3]
        secondcount = typecond[idx3 + flaglen:]
        idx1 = firstcount.rfind(')')
        idx2 = secondcount.rfind(')')

        si1 = SqlItem(firstcount[12:idx1], self.dbObj, 0)

        isgroup = False
        for i in range(0, len(si1.fieldobjs)):
            item = si1.fieldobjs[i]
            if not isgroup:
                aliasname = item.IDname
                idx = item.field_name.rfind(')')
                if idx > -1 and idx < len(item.field_name) - 2:
                    aliasname = item.field_name[idx + 2:]
                if item.field_name == '':
                    isgroup = True
                    continue
                if item.field_name == '1' or item.field_name == '*':
                    countstr += operation + ' (1) total_record'
                    newtypecond += ' and (' + ptab + '.total_record' + theflag + qtab + '.total_record) '
                elif item.isfunc:
                    countstr += operation + ' (' + item.funcname + ','.join(item.funcparas) + ')) total_' + aliasname
                    newtypecond += ' and (' + ptab + '.total_' + aliasname + theflag + qtab + '.total_' + aliasname + ')'
                elif aliasname != item.IDname:
                    countstr += operation + ' (' + item.field_name[:idx] + ') total_' + aliasname
                    newtypecond += ' and (' + ptab + '.total_' + aliasname + theflag + qtab + '.total_' + aliasname + ')'
                else:
                    countstr += operation + ' (' + item.field_name + ') total_' + aliasname
                    newtypecond += ' and (' + ptab + '.total_' + aliasname + theflag + qtab + '.total_' + aliasname + ')'
                countstr += ','
                # newtypecond += ' and '
            else:
                if item.field_name == '':
                    break
                if item.isfunc:
                    groupstr += item.funcname + '(' + ','.join(item.funcparas) + ')'
                    extra_conditions += ' and (' + item.addkeywithtab('a') + ' = ' + item.addkeywithtab(
                        ptab) + ') and (' \
                                        + item.addkeywithtab('b') + ' = ' + item.addkeywithtab(
                        qtab) + ') and (' + item.addkeywithtab(ptab) + ' = ' \
                                        + item.addkeywithtab(qtab) + ') '
                else:
                    groupstr += item.field_name
                    extra_conditions += ' and (a.' + item.field_name + ' = ' + ptab + '.' + item.field_name + ' ) and (b.' \
                                        + item.field_name + ' = ' + qtab + '.' + item.field_name + ') and ' + '(' + ptab + '.' + item.field_name + ' = ' + qtab + '.' + item.field_name + ') '
                groupstr += ','
        countstr = countstr.rstrip(',')
        groupstr = groupstr.rstrip(',')
        if newtypecond != '':
            newtypecond = newtypecond[len(' and '):]
        return groupstr, countstr, extra_conditions, newtypecond

    def getgroupstr4diffTypeUsedCountInfo(self, ptab, qtab):
        f1 = self.group_item.fieldobjs
        groupstr = ''
        countstr = ''
        extra_conditions = ''
        for it in f1:
            if it.isfunc:
                groupstr += it.funcname + '(' + ','.join(it.funcparas) + ')'
                extra_conditions += ' and (' + it.addkeywithtab('a') + ' = ' + it.addkeywithtab(ptab) + ') and (' \
                                    + it.addkeywithtab('b') + ' = ' + it.addkeywithtab(
                    qtab) + ') and (' + it.addkeywithtab(ptab) + ' = ' \
                                    + it.addkeywithtab(qtab) + ') '
            else:
                groupstr += it.field_name
                extra_conditions += ' and (a.' + it.field_name + ' = ' + ptab + '.' + it.field_name + ' ) and (b.' \
                                    + it.field_name + ' = ' + qtab + '.' + it.field_name + ') and ' + '(' + ptab + '.' + it.field_name + ' = ' + qtab + '.' + it.field_name + ') '
            groupstr += ','
        groupstr = groupstr.rstrip(',')
        return groupstr, extra_conditions

    def getcountstr4diffTypeUsedCountInfo(self, ptab, qtab):
        f2 = self.numeric_diff_item.fieldobjs
        f3 = self.count_item.fieldobjs
        countstr = ''
        for it in f2:
            aliasname = it.IDname
            idx00 = it.field_name.rfind(')')
            if idx00 > -1 and idx00 < len(it.field_name) - 1:
                aliasname = it.field_name[idx00 + 1:]
            if it.isfunc:
                countstr += 'sum(' + it.funcname + '(' + ','.join(it.funcparas) + ')) total_' + aliasname
            else:
                if aliasname == it.IDname:
                    countstr += 'sum(' + it.field_name + ') total_' + aliasname
                else:
                    countstr += 'sum(' + it.field_name[:idx00 + 1] + ') total_' + aliasname
            countstr += ','
        for it in f3:
            aliasname = it.IDname
            idx00 = it.field_name.rfind(')')
            if idx00 > -1 and idx00 < len(it.field_name) - 1:
                aliasname = it.field_name[idx00 + 1:]
            if it.field_name == '1' or it.field_name == '*':
                countstr += 'count(1) total_record'
            elif it.isfunc:
                countstr += 'count(' + it.funcname + '(' + ','.join(it.funcparas) + ')) total_' + aliasname
            else:
                if aliasname == it.IDname:
                    countstr += 'count(' + it.field_name + ') total_' + aliasname
                else:
                    countstr += 'count(' + it.field_name[:idx00 + 1] + ') total_' + aliasname
            countstr += ','
        countstr = countstr.rstrip(',')

        return countstr

    def make_tmp_table_sqls(self, difftypes, diffscenes, table_def, region_code, old_new_flag, used_fields='',
                            dblnk_str=''):
        sqlsmap = {}
        cntsqlmap = {}
        diffcond = ''
        typecond = ''
        wherecond = ''
        totalcond = ''
        ori_minus_keys, minus_keys = self.getOriMinusKeys(dblnk_str)
        used_fields1 = self.getRealUsedFields(table_def, used_fields, dblnk_str)
        if used_fields1 == '':
            used_fields1 = '*'
        used_fields2 = ',a.'.join(used_fields1.split(','))
        used_fields2 = 'a.' + used_fields2
        table_def1, table_def2, real_table1, real_table2 = self.getRealTableDef(table_def, region_code, used_fields,
                                                                                dblnk_str)
        table_def_first = table_def1
        table_def_second = table_def2
        real_table = real_table1
        if old_new_flag == 1:
            table_def_first = table_def2
            table_def_second = table_def1
            real_table = real_table2

        sqlsmap['init'] = []
        sql1 = 'drop table ' + real_table
        sqlsmap['init'].append(sql1)
        if old_new_flag == 0:
            sql1 = 'create table ' + real_table + ' as select * from (' + table_def1 + ') where rownum < 1'
            sqlsmap['init'].append(sql1)
        else:
            sql1 = 'create table ' + real_table + ' as select * from (' + table_def2 + ') where rownum < 1'
            sqlsmap['init'].append(sql1)
        sql1 = 'alter table ' + real_table + ' add diff_type number(4) default -1'
        sqlsmap['init'].append(sql1)
        sql1 = 'alter table ' + real_table + ' add diff_scene_id number(4) default -1'
        sqlsmap['init'].append(sql1)

        wherecond = self.minus_item.getwherecond('a', 'b')
        # for it in minus_keys.split(','):
        #    wherecond += 'a.'+it+' = '+'b.'+it + ' and '

        if diffscenes == {}:
            diffscenes = {'-1': ['', '']}
        if difftypes == {}:
            difftypes = {11: ''}
        for diffsceneId in diffscenes.keys():
            if diffscenes[diffsceneId][0].strip() != '':
                diffcond = ' (' + diffscenes[diffsceneId][0] + ' )'
            if diffscenes[diffsceneId][1].strip() != '':
                diffcond += ' and (' + diffscenes[diffsceneId][1] + ' )'

            totalcond = wherecond
            if diffcond != '':
                totalcond += ' and ' + diffcond

            sql1 = 'insert into ' + real_table + ' ( ' + used_fields1 + ',diff_type,diff_scene_id ) ' \
                   + ' select  ' + used_fields1 + ',' + str(
                11 + old_new_flag) + ' diff_type, ' + diffsceneId + ' diff_scene_id from ' \
                   + '(select ' + used_fields2 + ' from ( ' + table_def_first + ') a, ' \
                   + '((select ' + ori_minus_keys + ' from (' + table_def_first + '))   ' \
                   + 'minus (select ' + ori_minus_keys + ' from (' + table_def_second + '))) b where ' + totalcond + ') xxx'

            if diffsceneId not in sqlsmap.keys():
                sqlsmap[diffsceneId] = {}
            sqlsmap[diffsceneId]['11'] = [sql1]

            for difftypeId in difftypes.keys():
                typecond = difftypes[difftypeId]
                typecond = typecond.replace(' ax ', '(' + table_def_first + ') ax ').replace(' ay ',
                                                                                             '(' + table_def_second + ') ay ')

                totalcond = wherecond
                if diffcond != '':
                    totalcond += ' and ' + diffcond

                # sql1 = 'insert into ' + real_table + ' ( '+used_fields1+',diff_type,diff_scene_id ) '
                #       +' select '+used_fields1+', '+ str(difftypeId+old_new_flag)   +' diff_type, ' + diffsceneId +' diff_scene_id from '
                #       +'(select '  +used_fields2+' from ( '+ table_def_first+') a, '
                #       +'((select '+ori_minus_keys+' from ('+table_def_first+'))  '
                #       +' minus (select '+ori_minus_keys+' from ('+table_def_second+'))) b where '+totalcond+') xxx'
                if self.isDiffTypeUsedCount(difftypeId, difftypes[difftypeId]):
                    groupstr, extra_conditions = self.getgroupstr4diffTypeUsedCountInfo('c', 'd')
                    countstr = self.getcountstr4diffTypeUsedCountInfo('c', 'd')
                    groupstr, countstr, extra_conds, typecond1 = self.getgroups_counts_extras(difftypes[difftypeId],
                                                                                              'c', 'd')
                    if typecond1 != '':
                        typecond = typecond1
                    if typecond.strip() != '':
                        typecond = ' (' + typecond + ' )'
                        totalcond += ' and ' + typecond
                    if groupstr != '' and countstr != '':
                        sql1 = 'insert into ' + real_table + ' ( ' + used_fields1 + ',diff_type,diff_scene_id ) ' \
                               + ' select ' + used_fields1 + ', ' + str(
                            difftypeId + old_new_flag) + ' diff_type, ' + str(diffsceneId) \
                               + ' diff_scene_id from ' + '(select ' + used_fields2 + ' from ( ' + table_def_first + ') a) a, ' \
                               + '(select ' + used_fields2 + ' from (' + table_def_second + ') a) b,  ' \
                               + '(select ' + groupstr + ',' + countstr + ' from (' + table_def_first + ') group by ' + groupstr + ') c ,' \
                               + '(select ' + groupstr + ',' + countstr + ' from (' + table_def_second + ') group by ' + groupstr + ') d ' \
                               + ' where ' + totalcond + extra_conditions + ') xxx'
                    elif countstr != '':
                        sql1 = 'insert into ' + real_table + ' ( ' + used_fields1 + ',diff_type,diff_scene_id ) ' + ' select ' + used_fields1 + ', ' \
                               + str(difftypeId + old_new_flag) + ' diff_type, ' + str(
                            diffsceneId) + ' diff_scene_id from ' + '(select ' \
                               + used_fields2 + ' from ( ' + table_def_first + ') a) a, ' + '(select ' + used_fields2 + ' from (' + table_def_second + ') a) b,  ' \
                               + '(select ' + countstr + ' from (' + table_def_first + ') ) c ,' + '(select ' + countstr + ' from (' + table_def_second + ') ) d ' \
                               + ' where ' + totalcond + ') xxx'
                else:
                    if typecond != '':
                        typecond = ' (' + typecond + ' )'
                        totalcond += ' and ' + typecond
                    sql1 = 'insert into ' + real_table + ' ( ' + used_fields1 + ',diff_type,diff_scene_id ) ' \
                           + ' select ' + used_fields1 + ', ' + str(difftypeId + old_new_flag) + ' diff_type, ' + str(
                        diffsceneId) + ' diff_scene_id from ' \
                           + '(select ' + used_fields2 + ' from ( ' + table_def_first + ') a) a, ' \
                           + '(select ' + used_fields2 + ' from (' + table_def_second + ') a) b  ' \
                           + ' where ' + totalcond + ') xxx'
                sqlsmap[diffsceneId][str(difftypeId)] = [sql1]

            totalcond = diffcond
            for difftypeId in difftypes.keys():
                typecond = difftypes[difftypeId]
                typecond = typecond.replace(' ax ', '(' + table_def_first + ') ax ').replace(' ay ',
                                                                                             '(' + table_def_second + ') ay ')
                typecond1 = ''
                groupstr = countstr = extra_conds = typecond1 = ''
                if self.isDiffTypeUsedCount(difftypeId, difftypes[difftypeId]):
                    groupstr, extra_conditions = self.getgroupstr4diffTypeUsedCountInfo('c', 'd')
                    countstr = self.getcountstr4diffTypeUsedCountInfo('c', 'd')
                    groupstr, countstr, extra_conds, typecond1 = self.getgroups_counts_extras(difftypes[difftypeId],
                                                                                              'c', 'd')
                if typecond1 != '':
                    typecond = typecond1
                if typecond.strip() != '':
                    typecond = ' (' + typecond + ' )'
                if typecond != '':
                    totalcond += ' and ' + typecond
                ndifftypeId = '-1'
                if difftypeId != '-1':
                    ndifftypeId = str(difftypeId + old_new_flag)
                sql1 = ''
                if self.isDiffTypeUsedCount(difftypeId, difftypes[difftypeId]):
                    if groupstr != '' and countstr != '':
                        sql1 = "select %s,diff_type,diff_scene from ( select a.*," + ndifftypeId + " diff_type," \
                               + str(diffsceneId) + " diff_scene_id from  ( " + table_def_first + ' ) a, ' \
                               + '(select ' + ori_minus_keys + ' from (' + table_def_second + ')) b ' \
                               + '(select ' + groupstr + ',' + countstr + ' from (' + table_def_first + ') group by' + groupstr + ') c ,' \
                               + '(select ' + groupstr + ',' + countstr + ' from (' + table_def_second + ') group by' + groupstr + ') d ' \
                               + ' where ' + totalcond + extra_conds + ') xxx'
                    else:
                        sql1 = "select %s,diff_type,diff_scene from ( select a.*," + ndifftypeId + " diff_type," \
                               + str(diffsceneId) + " diff_scene_id from  ( " + table_def_first + ' ) a, ' \
                               + '(select ' + ori_minus_keys + ' from (' + table_def_second + ')) b ' \
                               + '(select ' + countstr + ' from (' + table_def_first + ') ) c ,' \
                               + '(select ' + countstr + ' from (' + table_def_second + ') ) d ' \
                               + ' where ' + totalcond + extra_conds + ') xxx'
                else:
                    sql1 = "select %s,diff_type,diff_scene from ( select a.*," + ndifftypeId + " diff_type," \
                           + str(diffsceneId) + " diff_scene_id from  ( " + table_def_first + ' ) a, ' \
                           + '(select ' + ori_minus_keys + ' from (' + table_def_second + ')) b '
                    if totalcond.strip() != '':
                        sql1 += 'where ' + totalcond + ') xxx'
                    else:
                        sql1 += ' ) xxx'
                if self.user_table_name not in cntsqlmap.keys():
                    cntsqlmap[self.user_table_name] = {}
                if diffsceneId not in cntsqlmap[self.user_table_name].keys():
                    cntsqlmap[self.user_table_name][diffsceneId] = {}
                cntsqlmap[self.user_table_name][diffsceneId][difftypeId] = sql1
        prints(sqlsmap, cntsqlmap)
        return sqlsmap, cntsqlmap

    def make_diff_sql2(self, casetype, region_code, version, dblnk_str=''):
        sqlsmap = {}
        tablename = self.user_table_name
        tablename = self.concat_tablename_regioncode(self.user_table_name, region_code)

        recent_dates = self.tnc.getprobablydays()
        old_table_name = self.make_tmp_table_names(tablename, region_code, recent_dates, dblnk_str, 0)
        new_table_name = self.make_tmp_table_names(tablename, region_code, recent_dates, '', 1)
        splitor = GlobalVar.key_split_char_insel
        tkey = self.key_item.generatekeyinfo(tablename, splitor, '_key')
        tdiff1 = SqlItem.concatdetails(self.dbObj.DBTYPE, \
                                       self.diff_item.generatekeyinfo(tablename, splitor, '_old'), \
                                       self.numeric_diff_item.generatekeyinfo(tablename, splitor, '_old'), splitor)
        tdiff2 = SqlItem.concatdetails(self.dbObj.DBTYPE, \
                                       self.diff_item.generatekeyinfo(tablename, splitor, '_new'), \
                                       self.numeric_diff_item.generatekeyinfo(tablename, splitor, '_new'), splitor)
        tdiff3 = SqlItem.concatdetails(self.dbObj.DBTYPE, \
                                       self.diff_item.generatekeyinfo(tablename, splitor, '_diff'), \
                                       self.numeric_diff_item.generatekeyinfo(tablename, splitor, '_diff'), splitor)
        # casetype,difftype,diffscene,regioncode,version = extra_info
        roottcId = self.getroottc(self.tnc.caseId, self.dbObj)
        sql1 = '''insert into %s at_tc_dbcmp_diff_detail (TASK_ID,TC_ID,TYPE,diff_type,diff_scene,sub_tc_id,region_code,Version,Table_Name,Key_Info,Old_Info,New_Info,Diff_Info) 
                (select %s '%s' task_id, '%s' tc_id, '%s' type, diff_type, diff_scene, '%s' sub_tc_id, '%s' region_code,  version, '%s' table_name,%s,
                 %s,%s,%s from ''' % (
        self.getparrelstr(), self.getparrelstr(), str(self.tnc.taskId), str(roottcId), str(casetype),
        str(self.tnc.caseId), str(region_code), tablename, tkey, tdiff1, tdiff2, tdiff3)
        self.numeric_diff_item.setcountNow(0)
        union10 = unioncolumns(self.diff_item.getdiffsql('a0', 'b0'), \
                               self.numeric_diff_item.getdiffsql('a0', 'b0'), \
                               GlobalVar.key_split_char, 0)
        union11 = unioncolumns(self.diff_item.getnormaldiff('a1'), \
                               self.numeric_diff_item.getnormaldiff('a1'), \
                               GlobalVar.key_split_char, 0)
        union12 = unioncolumns(self.diff_item.getnegdiff('b2'), \
                               self.numeric_diff_item.getnegdiff('b2'), \
                               GlobalVar.key_split_char, 0)
        union0 = unioncolumns(self.key_item.getkeyswithfea('a0', '_key'), union10, GlobalVar.key_split_char, 0)
        union1 = unioncolumns(self.key_item.getkeyswithfea('a1', '_key'), union11, GlobalVar.key_split_char, 0)
        union2 = unioncolumns(self.key_item.getkeyswithfea('b2', '_key'), union12, GlobalVar.key_split_char, 0)
        whereconditions0 = self.key_item.getwherecond('a0', 'b0')

        sql2 = "(select %s 0 version,a0.diff_type diff_type,a0.diff_scene_id diff_scene,%s from %s a0,%s b0 where ( %s )" \
               " and a0.diff_type != 11 and b0.diff_type != 12 and a0.diff_type + 1 = b0.diff_type) " % (
               self.getparrelstr(), union0, old_table_name, new_table_name, whereconditions0)
        sql3 = "union all (select %s 0 version, a1.diff_type diff_type,a1.diff_scene_id diff_scene, %s from %s a1  where a1.diff_type = 11)  " % (
        self.getparrelstr(), union1, old_table_name)
        sql4 = "union all  (select %s 0 version,b2.diff_type diff_type,b2.diff_scene_id diff_scene,%s from %s b2  where b2.diff_type = 12)  ) " % (
        self.getparrelstr(), union2, new_table_name);
        sql = sql1 + '( ' + sql2 + sql3 + sql4 + ' )'
        return sql

    def make_diff_sum_sql2(self, region_code, casetype, dblnk_str='', withGroups=False):
        tablename = self.user_table_name
        tablename = self.concat_tablename_regioncode(self.user_table_name, region_code)
        real_table = tablename
        recent_dates = self.tnc.getprobablydays()
        old_table_name = self.make_tmp_table_names(tablename, region_code, recent_dates, dblnk_str, 0)
        new_table_name = self.make_tmp_table_names(tablename, region_code, recent_dates, '', 1)

        roottcId = self.getroottc(self.tnc.caseId, self.dbObj)
        sumkeys = self.sum_item.recs
        wheres = self.wherecond

        self.numeric_diff_item.setcountNow(1)
        wherestr = ' where ' if self.sum_item.recs != '' else ''
        splitor = GlobalVar.key_split_char_insel
        tsumkey = self.sum_item.generatekeyinfo(tablename, splitor, '_key')
        tsumdiff1 = SqlItem.concatdetails(self.dbObj.DBTYPE, \
                                          self.numeric_diff_item.generatekeyinfo(tablename, splitor, '_old_sum'),
                                          self.count_item.generatekeyinfo(tablename, splitor, '_old_sum'), splitor)
        tsumdiff2 = SqlItem.concatdetails(self.dbObj.DBTYPE, \
                                          self.numeric_diff_item.generatekeyinfo(tablename, splitor, '_new_sum'),
                                          self.count_item.generatekeyinfo(tablename, splitor, '_new_sum'), splitor)
        tsumdiff3 = SqlItem.concatdetails(self.dbObj.DBTYPE, \
                                          self.numeric_diff_item.generatekeyinfo(tablename, splitor, '_diff_sum'),
                                          self.count_item.generatekeyinfo(tablename, splitor, '_diff_sum'), splitor)
        union0 = union1 = union2 = ''
        if sumkeys != '':
            union0 = unioncolumns(self.sum_item.getkeyswithfea('a0', '_key'), \
                                  self.numeric_diff_item.getdiffsql('a0',
                                                                    'b0') + GlobalVar.key_split_char + self.count_item.getdiffsql(
                                      'a0', 'b0'), \
                                  GlobalVar.key_split_char, 0)
            union1 = unioncolumns(self.sum_item.getkeyswithfea('a1', '_key'), \
                                  self.numeric_diff_item.getnormaldiff(
                                      'a1') + GlobalVar.key_split_char + self.count_item.getnormaldiff('a1'), \
                                  GlobalVar.key_split_char, 0)
            union2 = unioncolumns(self.sum_item.getkeyswithfea('b2', '_key'), \
                                  self.numeric_diff_item.getnormaldiff(
                                      'b2') + GlobalVar.key_split_char + self.count_item.getnormaldiff('b2'), \
                                  GlobalVar.key_split_char, 0)
        else:
            union0 = unioncolumns('', self.numeric_diff_item.getdiffsql('a0',
                                                                        'b0') + GlobalVar.key_split_char + self.count_item.getdiffsql(
                'a0', 'b0'), GlobalVar.key_split_char, 0)
            if union0 == '':
                union0 = "''"
            union1 = unioncolumns(self.sum_item.getkeyswithfea('a1', '_key'), \
                                  self.numeric_diff_item.getnormaldiff(
                                      'a1') + GlobalVar.key_split_char + self.count_item.getnormaldiff('a1'), \
                                  GlobalVar.key_split_char, 0)
            if union1 == '':
                union1 = "''"
            union2 = unioncolumns(self.sum_item.getkeyswithfea('b2', '_key'), \
                                  self.numeric_diff_item.getnormaldiff(
                                      'b2') + GlobalVar.key_split_char + self.count_item.getnormaldiff('b2'), \
                                  GlobalVar.key_split_char, 0)
            if union2 == '':
                union2 = "''"
        group_splitor = ''

        if not withGroups:
            if self.sum_item.recs != '':
                group_splitor = ','
            sql1 = '''insert into at_tc_dbcmp_diff_stat (TASK_ID,TC_ID,TYPE,diff_type,diff_scene,sub_tc_id,region_code,Version,Table_Name,Key_Info,Old_Info,New_Info,Diff_Info) 
                      (select '%s' task_id, '%s' tc_id, '%s' type, diff_type, diff_scene, '%s' sub_tc_id, '%s' region_code,  version, '%s' table_name,%s, %s, %s, %s from (''' % (
            self.tnc.taskId, roottcId, casetype, self.tnc.caseId, region_code, real_table, tsumkey, tsumdiff1,
            tsumdiff2, tsumdiff3)
            sql2 = "(select 0 version, a0.diff_type diff_type,a0.diff_scene_id diff_scene, %s from %s a0, %s b0 where %s and a0.diff_type != 11 and b0.diff_type != 12 and a0.diff_type + 1 = b0.diff_type  group by a0.diff_scene_id,a0.diff_type %s %s) " % (
            union0, old_table_name, new_table_name, self.key_item.getwherecond('a0', 'b0'), group_splitor,
            self.sum_item.getgroups('a0', False))
            sql3 = " union all (select 0 version, a1.diff_type diff_type,a1.diff_scene_id diff_scene, %s from %s a1 where a1.diff_type = 11 group by a1.diff_scene_id, diff_type %s %s)" % (
            union1, old_table_name, group_splitor, self.sum_item.getgroups('a1', False))
            sql4 = " union all (select 0 version, b2.diff_type diff_type,b2.diff_scene_id diff_scene, %s from %s b2 where b2.diff_type = 12  group by b2.diff_scene_id, diff_type %s %s) ) )" % (
            union2, new_table_name, group_splitor, self.sum_item.getgroups('b2', False))

            sql = sql1 + sql2 + sql3 + sql4
        else:
            if self.group_item.recs != '':
                group_splitor = ','
            sql1 = '''insert into at_tc_dbcmp_diff_stat1 (TASK_ID,TC_ID,TYPE,diff_type,diff_scene,sub_tc_id,region_code,Version,Table_Name,Key_Info,Old_Info,New_Info,Diff_Info) 
                      (select '%s' task_id, '%s' tc_id, '%s' type, diff_type, diff_scene, '%s' sub_tc_id, '%s' region_code,  version, '%s' table_name,%s, %s, %s, %s from (''' % (
            self.tnc.taskId, roottcId, casetype, self.tnc.caseId, region_code, real_table, tsumkey, tsumdiff1,
            tsumdiff2, tsumdiff3)
            sql2 = "(select 0 version, a0.diff_type diff_type,a0.diff_scene_id diff_scene, %s from %s a0, %s b0 where %s and a0.diff_type != 11 and b0.diff_type != 12 and a0.diff_type + 1 = b0.diff_type  group by a0.diff_scene_id,a0.diff_type %s %s) " % (
            union0, old_table_name, new_table_name, self.key_item.getwherecond('a0', 'b0'), group_splitor,
            self.group_item.getgroups('a0', False))
            sql3 = " union all (select 0 version, a1.diff_type diff_type,a1.diff_scene_id diff_scene, %s from %s a1 where a1.diff_type = 11 group by a1.diff_scene_id, diff_type %s %s)" % (
            union1, old_table_name, group_splitor, self.group_item.getgroups('a1', False))
            sql4 = " union all (select 0 version, b2.diff_type diff_type,b2.diff_scene_id diff_scene, %s from %s b2 where b2.diff_type = 12  group by b2.diff_scene_id, diff_type %s %s) ) )" % (
            union2, new_table_name, group_splitor, self.group_item.getgroups('b2', False))

            sql = sql1 + sql2 + sql3 + sql4
        return sql

    def init_map(self, map, table_name, region_code, diff_scene, diff_type):
        if table_name not in map.keys():
            map[table_name] = {}
        if region_code not in map[table_name].keys():
            map[table_name][region_code] = {}
        if diff_scene not in map[table_name][region_code].keys():
            map[table_name][region_code][diff_scene] = {}
        if diff_type not in map[table_name][region_code][diff_scene].keys():
            map[table_name][region_code][diff_scene][diff_type] = {}

    def iscountitem(self, key):
        for k in self.cntkeys.split(','):
            if key.lower().strip() == GlobalVar.CHAR_INSTEAD_OF_CNT and self.cntkeys == '*':
                return True
            if key.lower().strip() == k.lower().strip():
                return True
        return False

    def issumitem(self, key):
        for k in self.sumkeys.split(','):
            if key.lower().strip() == k.lower().strip():
                return True
        return False

    def analyse_count_sum(self, info_str):
        count_str = ''
        sum_str = ''
        lst = info_str.split(',')
        for it in lst:
            slst = it.split('=')
            key = slst[0]
            val = slst[1]
            if self.iscountitem(key):
                count_str += it + ','
            elif self.issumitem(key):
                sum_str += it + ','
        if count_str != '':
            count_str = count_str[:-1]
        if sum_str != '':
            sum_str = sum_str[:-1]
        return count_str, sum_str

    def make_stat_maps(self):
        sql = "select key_info,old_info,new_info,diff_info,table_name,region_code,diff_scene,diff_type \
               from at_tc_dbcmp_diff_stat where task_id = '%s' and tc_id = '%s' and (length(region_code) > 0 or length(diff_scene) > 0) and not( region_code = -1 and diff_scene  = -1)" % (
        str(self.tnc.taskId), str(self.tnc.caseId))
        lst, msg = self.dbObj.execute(sql)
        count_map_old = count_map_new = sum_map_old = sum_map_new = sum_map_keys = diff_count_map = diff_sum_map = {}
        for item in lst:
            table_name = item[4]
            region_code = item[5]
            diff_scene = item[6]
            diff_type = item[7]
            for map in (
            count_map_old, count_map_new, sum_map_old, sum_map_new, sum_map_keys, diff_count_map, diff_sum_map):
                self.init_map(map, table_name, region_code, diff_scene, diff_type)

            old_info = item[1]
            new_info = item[2]
            diff_info = item[3]
            key_info = item[0]
            sum_map_keys[table_name][region_code][diff_scene][diff_type] = key_info

            old_count_str, old_sum_str = self.analyse_count_sum(old_info)
            new_count_str, new_sum_str = self.analyse_count_sum(new_info)
            diff_count_str, diff_sum_str = self.analyse_count_sum(diff_info)

            count_map_old[table_name][region_code][diff_scene][diff_type] = old_count_str
            count_map_new[table_name][region_code][diff_scene][diff_type] = new_count_str
            sum_map_old[table_name][region_code][diff_scene][diff_type] = old_sum_str
            sum_map_new[table_name][region_code][diff_scene][diff_type] = new_sum_str
            diff_count_map[table_name][region_code][diff_scene][diff_type] = diff_count_str
            diff_sum_map[table_name][region_code][diff_scene][diff_type] = diff_sum_str

        return count_map_old, count_map_new, sum_map_old, sum_map_new, sum_map_keys, diff_count_map, diff_sum_map

    def caculate_counts(self, count_map, sceneId='', diff_type=''):
        total_map = {}
        for table_name in count_map.keys():
            if table_name not in total_map.keys():
                total_map[table_name] = {}
            for region_code in count_map[table_name].keys():
                if sceneId not in count_map[table_name][region_code].keys():
                    continue
                if diff_type in count_map[table_name][region_code][sceneId].keys():
                    countinfo = count_map[table_name][region_code][sceneId][diff_type]
                    countlst = countinfo.split(',')
                    m = {}
                    for it in countlst:
                        sit = it.split('=')
                        key = sit[0].strip()
                        val = int(sit[1].strip())
                        m[key] = val
                    for k in m.keys():
                        if k not in total_map[table_name].keys():
                            total_map[table_name][k] = m[k]
                        else:
                            total_map[table_name][k] += m[k]

        return total_map

    def caculate_sums(self, sum_map, sceneId='', diff_type=''):
        total_sum_map = {}
        for table_name in sum_map.keys():
            if table_name not in sum_map.keys():
                total_sum_map[table_name] = {}
            for region_code in sum_map[table_name].keys():
                if sceneId not in sum_map[table_name][region_code].keys():
                    continue
                if diff_type not in sum_map[table_name][region_code][sceneId].keys():
                    continue
                suminfo = sum_map[table_name][region_code][sceneId][diff_type]
                sumlst = suminfo.split(',')
                m = {}
                for it in sumlst:
                    sit = it.split('=')
                    key = sit[0].strip()
                    val = int(sit[1].strip())
                    m[key] = val
                for k in m.keys():
                    if k not in total_sum_map[table_name].keys():
                        total_sum_map[table_name][k] = m[k]
                    else:
                        total_sum_map[table_name][k] += m[k]

        return total_sum_map

    def convert_map_to_str(self, total_map, total_sum_map, ref_total_map, ref_total_sum_map):
        strinfo = ''
        for k in total_map.keys():
            strinfo += '%s=' + str(total_map[k]) + ',' % (k)
        if strinfo == '':
            for k in ref_total_map.keys():
                strinfo += '%s=0,' % (k)
        for k in total_sum_map.keys():
            strinfo += '%s=' + str(total_sum_map[k]) + ',' % (k)
        if total_sum_map == {}:
            for k in ref_total_sum_map.keys():
                strinfo += '%s=0,' % (k)
        if strinfo != '':
            strinfo = strinfo[:-1]
        return strinfo

    def convert_map_to_diff_str(self, total_map_1, total_map_2, total_sum_map_1, total_sum_map_2):
        strinfo = ''
        for k in total_map_1.keys():
            if k in total_map_2.keys():
                strinfo += '%s=' + str(total_map_1[k]) + '|' + str(total_map_2[k]) + ',' % (k)
            else:
                strinfo += '%s=' + str(total_map_1[k]) + '|0,' % (k)
        for k in total_map_2.keys():
            if k not in total_map_1.keys():
                strinfo += '%s=0|' + str(total_map_2[k]) + ',' % (k)

        for k in total_sum_map_1.keys():
            if k in total_sum_map_2.keys():
                strinfo += '%s=' + str(total_sum_map_1[k]) + '|' + str(total_sum_map_2[k]) + ',' % (k)
            else:
                strinfo += '%s=' + str(total_sum_map_1[k]) + '|0,' % (k)
        for k in total_sum_map_2.keys():
            if k not in total_sum_map_1.keys():
                strinfo += '%s=0|' + str(total_sum_map_2[k]) + ',' % (k)
        if strinfo != '':
            strinfo = strinfo[:-1]
        return strinfo

    def genererate_total_sql(self, table_name, count_map, sum_map, ref_count_map, ref_sum_map):
        strinfo = ''
        count_map_1 = {}
        sum_map_1 = {}
        ref_count_map_1 = {}
        ref_sum_map_1 = {}

        if table_name in count_map.keys():
            count_map_1 = count_map[table_name]
        if table_name in sum_map.keys():
            sum_map_1 = sum_map[table_name]
        if table_name in ref_count_map.keys():
            ref_count_map_1 = ref_count_map[table_name]
        if table_name in ref_sum_map.keys():
            ref_sum_map_1 = ref_sum_map[table_name]
        strinfo = self.convert_map_to_str(count_map_1, sum_map_1, ref_count_map_1, ref_sum_map_1)

        return strinfo

    def generate_total_diff_sql(self, table_name, count_map_1, count_map_2, sum_map_1, sum_map_2):
        strinfo = ''
        c1 = c2 = s1 = s2 = {}
        if table_name in count_map_1.keys():
            c1 = count_map_1[table_name]
        if table_name in count_map_2.keys():
            c2 = count_map_2[table_name]
        if table_name in sum_map_1.keys():
            s1 = sum_map_1[table_name]
        if table_name in sum_map_2.keys():
            s2 = sum_map_2[table_name]
        strinfo = self.convert_map_to_diff_str(c1, c2, s1, s2)
        return strinfo

    def make_total_sql(self, count_map_pair, sum_map_pair, sceneId, diff_type, version='0'):
        old_count_map, new_count_map = count_map_pair
        old_sum_map, new_sum_map = sum_map_pair
        sqlsmap = {}
        old_more_tables = []
        new_more_tables = []
        table_names = []
        casetype = -1
        sql = '''insert into at_tc_dbcmp_diff_stat  (TASK_ID,TC_ID,TYPE,diff_type,diff_scene,sub_tc_id,region_code,Version,Table_Name,Key_Info,Old_Info,New_Info,Diff_Info) values('%s','%s','%s','%s','%s','%s','','%s','%s','%s','%s','%s','%s')'''
        key_info = new_info = diff_info = old_info = ''

        for table_name in old_count_map.keys():
            if table_name not in table_names:
                table_names.append(table_name)
        for table_name in new_count_map.keys():
            if table_name not in table_names:
                table_names.append(table_name)
        for table_name in old_sum_map.keys():
            if table_name not in table_names:
                table_names.append(table_name)
        for table_name in new_sum_map.keys():
            if table_name not in table_names:
                table_names.append(table_name)

        for table_name in table_names:
            old_info = self.genererate_total_sql(table_name, old_count_map, old_sum_map, new_count_map, new_sum_map)
            new_info = self.genererate_total_sql(table_name, new_count_map, new_sum_map, old_count_map, old_sum_map)

            diff_info = self.generate_total_diff_sql(table_name, old_count_map, new_count_map, old_sum_map, new_sum_map)
            sql1 = sql % (
            self.tnc.taskId, self.tnc.caseId, casetype, diff_type, sceneId, self.tnc.caseId, str(version), table_name,
            key_info, old_info, new_info, diff_info)
            if key_info.strip() != '':
                sqlsmap[table_name] = sql1

        return sqlsmap


# run_flows = [self.make_drop_table_sql, self.make_rename_oritable_sql,self.make_minus_table,self.delete_result_sql,\
#                      self.make_diff_sql_4table,self.make_sum_sql_4table,self.make_count_each_table_sql,\
#                      self.make_sum_each_table_sql,self.update_result_sql,self.move_result_sql]
def replace_env_flag(sql, env_type):
    sql1 = sql
    sql1 = sql1.replace('ENV_BEGIN ', '')
    sql1 = sql1.replace(' ENV_END', '')
    return sql1


def getRootTcId(caseId, dbObj):
    sql0 = "select root_tc_id from at_tc where tc_id = '%s'" % (caseId)
    lst, msg = dbObj.execute(sql0)
    if len(lst) > 0:
        return lst[0][0]
    return -1


def sureTcStopping(tc_id, dbObj):
    sql = "select status from at_tc_task_tc where tc_id = %s" % (str(tc_id))
    lst, msg = dbObj.execute(sql)
    status = STATUS_MAP['ready']
    if len(lst) > 0:
        status = lst[0][0]
    if status in (STATUS_MAP['canceling'], STATUS_MAP['canceled']):
        return False
    return True


def sureOldEnvRunOver(task_id, tc_id, dbObj):
    # return True
    while True:
        sql = "select status from at_tc_task_tc where task_id = %s and tc_id = ( select a.tc_id from at_tc a, at_tc b where a.parent_tc_id = b.parent_tc_id and a.tc_id != b.tc_id and b.tc_id = %s)"
        sql = sql % (task_id, tc_id)
        lst, msg = dbObj.execute(sql)
        status = STATUS_MAP['ready']
        if len(lst) > 0:
            status = lst[0][0]
        if status in (STATUS_MAP['canceling'], STATUS_MAP['canceled']):
            prints('sureOldEnvRunOver:', status, task_id, tc_id)
            return False
        if status in (STATUS_MAP['finished'],):
            prints('sureOldEnvRunOver:', status, task_id, tc_id)
            return True
        time.sleep(5)
    prints('sureOldEnvRunOver:failed')
    return False


def waiting4threads(time_sleep_interval, tlst, tc_id, dbObj, runflagfunc=None):
    tlst1 = []
    runflag = True
    prints('waiting4threads start:', tlst, tc_id)
    while runflag:
        tlst1 = []
        if runflagfunc != None:
            runflag = runflagfunc(tc_id, dbObj)
        else:
            runflag = sureTcStopping(tc_id, dbObj)
        for t in tlst:
            if t.is_alive():
                time.sleep(time_sleep_interval)
            else:
                tlst1.append(t)
        for t in tlst1:
            tlst.remove(t)
        if tlst == []:
            break
    if not runflag:
        for t in tlst:
            try:
                if t.is_alive():
                    t.terminate()
            except Exception as e:
                prints(str(e))
    prints('waiting4threads end:', tlst, tc_id)


def execute_one_table(table_name, sqlsmap, dbObj, env_type, msgmap):
    sql1tuple = sqlsmap[table_name]
    sql1 = sql2 = ''
    if str(type(sql1tuple)).find('tuple') > -1:
        sql1, sql2 = sql1tuple
        sql1 = replace_env_flag(sql1, env_type)
        sql2 = replace_env_flag(sql2, env_type)
    else:
        sql2 = sql1tuple
        sql2 = replace_env_flag(sql2, env_type)

    sqllst = []
    if env_type == TestDBEnvManager.ENV_TYPE_MAP['old']:
        sqllst = sql1.split(';\n')
    else:
        sqllst = sql2.split(';\n')
    bSucc = True
    for sql in sqllst:
        lst, msg = dbObj.execute(sql)
        if dbObj.executefailed(msg):
            if not dbObj.shouldignored(sql, msg, DBObj.IGNORED_COMMAND):
                bSucc = False
                msgmap[table_name] = msg + sql
                return bSucc, msg + sql

    return bSucc, ''


def runsqlsOnebyOne(sqlsmap, dbObj, tc_id, env_type, parell_running=False):
    bSucc = True
    tlst = []
    msgmap = {}
    if str(type(sqlsmap)).lower().find('list') > -1:
        for sql in sqlsmap:
            ret, msg = dbObj.execute(sql)
            if dbObj.executefailed(msg):
                if not dbObj.shouldignored(sql, msg, DBObj.IGNORED_COMMAND):
                    return False, msg
        return True, ''

    for table_name in sqlsmap.keys():
        if parell_running:
            t = multiprocessing.Process(target=execute_one_table, args=(table_name, sqlsmap, dbObj, env_type, msgmap))
            tlst.append(t)
            t.start()
        else:
            ret, msg = execute_one_table(table_name, sqlsmap, dbObj, env_type, msgmap)
            if not ret:
                return ret, msg

    if parell_running:
        waiting4threads(3, tlst, tc_id, dbObj)
        if msgmap == {}:
            return True, ''
        return False, str(msgmap)
    return True, ''


def split_connstr_pair(connstr_pairs):
    old_conn_str = new_conn_str = ''
    if len(connstr_pairs) == 1:
        old_conn_str = new_conn_str = connstr_pairs[0]
    else:
        old_conn_str, new_conn_str = connstr_pairs[:2]
    return old_conn_str, new_conn_str


def get_all_dblnks(dbObj):
    dblnk_str_old = dblnk_str_new = dblnk_test_str = ''
    sql = "select lower(para_name), para_value from at_sys_base_para where lower(para_name) in ('dblnk_old','dblnk_new','dblnk_test')"
    lst, msg = dbObj.execute(sql)
    for it in lst:
        if it[1] == None:
            continue
        if it[0] == 'dblnk_old':
            dblnk_str_old = it[1]
        if it[0] == 'dblnk_new':
            dblnk_str_new = it[1]
        if it[0] == 'dblnk_test':
            dblnk_test_str = it[1]
    return dblnk_str_old, dblnk_str_new, dblnk_test_str


def dbcompare4envs(test_connstr_pair, user_table_name, env_type, version='', casetype='-1', caseinfo=()):
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    connstr_pairs = test_connstr_pair.split(',')
    old_conn_str, new_conn_str = split_connstr_pair(connstr_pairs)
    olddbObj = DBObj(old_conn_str)
    if old_conn_str != new_conn_str:
        newdbObj = DBObj(new_conn_str)
    else:
        newdbObj = olddbObj

    thedbObj = olddbObj
    if env_type == TestDBEnvManager.ENV_TYPE_MAP['new']:
        thedbObj = newdbObj

    table_name = user_table_name
    tcd = TabClustDef(user_table_name, dbObj)
    tnc = TableNameConvertor(dbObj, projId, taskId, caseId, stepId)
    tcd.setTNC(tnc)
    tcd.setbusiId()
    busiId = tcd.busi_id
    tcd = tcd.generatedbdefobj(casetype, caseId)
    thedblnk = TestDBEnvManager.getTestDBEnvManager(dbObj, tnc.projId).dblnk2.dblnk
    if env_type == TestDBEnvManager.ENV_TYPE_MAP['old']:
        thedblnk = TestDBEnvManager.getTestDBEnvManager(dbObj, tnc.projId).dblnk1.dblnk
    tcd.setkeyitemtype(thedblnk)
    tcd.redefine_table_define()
    tcd.get_last_table_define()

    dropsqlmap = tcd.make_drop_table_sql()
    ret, msg = runsqlsOnebyOne(dropsqlmap, thedbObj, caseId, env_type)
    retcode, renamesqlmap = tcd.make_rename_oritable_sql()
    if ret:
        ret, msg = runsqlsOnebyOne(renamesqlmap, thedbObj, caseId, env_type)
    else:
        return ret, msg
    if not ret:
        return ret, msg

    thedblnk = TestDBEnvManager.getTestDBEnvManager(dbObj, tnc.projId).dblnk2.dblnk

    # minussqlmap,diff_keys_info = tcd.make_minus_table(table_name,'',minus1,minus2)
    diff_info_sql_map = {}
    region_code_lst = ALL_REGION_CODES

    if tcd.table_define.find('$regionCodeEach') == -1:
        region_code_lst = [-1]
    for region_code in region_code_lst:
        diffscenes = tcd.generate_condstr_forscene('', region_code)
        difftypes = tcd.generate_condstr_fordifftype('', region_code)
        table_def = ''
        if str(region_code) in tcd.last_table_define_pair_map.keys():
            if len(tcd.last_table_define_pair_map[str(region_code)]) > 0:
                table_def = tcd.last_table_define_pair_map[str(region_code)][0][env_type]
        sqlmap, cntsqlmap = tcd.make_tmp_table_sqls(difftypes, diffscenes, table_def, region_code, env_type, '',
                                                    thedblnk)
        hsyslog.info('dbcompare4env:sqlmap is:' + str(sqlmap))
        for k in sqlmap.keys():
            if k not in diff_info_sql_map.keys():
                diff_info_sql_map[k] = sqlmap[k]
            else:
                if k == 'init':
                    diff_info_sql_map[k].extend(sqlmap[k])
                else:
                    for t in sqlmap[k].keys():
                        if t not in diff_info_sql_map[k].keys():
                            diff_info_sql_map[k][t] = sqlmap[k][t]
                        else:
                            diff_info_sql_map[k][t].extend(sqlmap[k][t])
    diffsqlmap = {}
    sumsqlmap = {}
    sumsqlmap2 = {}
    for region_code in region_code_lst:
        sql = tcd.make_diff_sql2(casetype, region_code, version, thedblnk)
        diffsqlmap[region_code] = sql
        sql = tcd.make_diff_sum_sql2(region_code, casetype, thedblnk)
        sumsqlmap[region_code] = sql
        sql = tcd.make_diff_sum_sql2(region_code, casetype, thedblnk, True)
        sumsqlmap2[region_code] = sql
    delresultmap = tcd.make_delete_result_sql()
    counttablemap = tcd.make_count_each_tabledef_sql(thedblnk)
    sumtablemap = tcd.make_sum_each_tabledef_sql(thedblnk)
    updateresultmap = tcd.make_update_result_sql()

    thedblnk1 = thedblnk
    if thedblnk1 != '' and thedblnk1[0] != '@':
        thedblnk1 = '@' + thedblnk1
    moveresultmap = tcd.make_move_result_sql(thedblnk1)

    if ret:
        for k in diff_info_sql_map.keys():
            if k != 'init':
                continue
            if ret:
                ret, msg = runsqlsOnebyOne(diff_info_sql_map[k], thedbObj, caseId, env_type)

    if env_type == TestDBEnvManager.ENV_TYPE_MAP['new']:
        if ret:
            ret = sureOldEnvRunOver(tnc.taskId, tnc.caseId, dbObj)
        if ret:
            ret, msg = runsqlsOnebyOne(delresultmap, thedbObj, caseId, env_type)
        if ret:
            ret, msg = runsqlsOnebyOne(delresultmap, dbObj, caseId, env_type)
        if ret:
            for k in diff_info_sql_map.keys():
                if k == 'init':
                    continue
                if ret:
                    for t in diff_info_sql_map[k].keys():
                        if ret:
                            ret, msg = runsqlsOnebyOne(diff_info_sql_map[k][t], thedbObj, caseId, env_type)
        if ret:
            ret, msg = runsqlsOnebyOne(diffsqlmap, thedbObj, caseId, env_type)
        if ret:
            ret, msg = runsqlsOnebyOne(sumsqlmap, thedbObj, caseId, env_type)
        if ret:
            ret, msg = runsqlsOnebyOne(sumsqlmap2, thedbObj, caseId, env_type)
        if ret:
            for tabname in updateresultmap.keys():
                # if ret:
                ret, msg = runsqlsOnebyOne(updateresultmap[tabname], thedbObj, caseId, env_type)
        if ret:
            for tabname in counttablemap.keys():
                for region_code in counttablemap[tabname].keys():
                    for scene_id in counttablemap[tabname][region_code].keys():
                        if ret:
                            ret, msg = runsqlsOnebyOne(counttablemap[tabname][region_code][scene_id], thedbObj, caseId,
                                                       env_type)

        if ret:
            for tabname in sumtablemap.keys():
                for region_code in sumtablemap[tabname].keys():
                    for scene_id in sumtablemap[tabname][region_code].keys():
                        if ret:
                            ret, msg = runsqlsOnebyOne(sumtablemap[tabname][region_code][scene_id], thedbObj, caseId,
                                                       env_type)
        if ret:
            ret, msg = runsqlsOnebyOne(moveresultmap, dbObj, caseId, env_type)
        if ret:
            count_map_old, count_map_new, sum_map_old, sum_map_new, sum_map_keys, diff_count_map, diff_sum_map = tcd.make_stat_maps()
            region_code = str(region_code_lst[0])
            diffscenes = tcd.generate_condstr_forscene('', region_code)
            difftypes = tcd.generate_condstr_fordifftype('', region_code)
            if diffscenes == {}:
                diffscenes = {-1: ['', '']}
            if difftypes == {}:
                difftypes = {11: ''}
            for sceneId in diffscenes.keys():
                for diff_type in difftypes.keys():
                    if int(diff_type) % 2 == 0:
                        continue
                    total_count_old_map = tcd.caculate_counts(count_map_old, sceneId, diff_type)
                    total_count_new_map = tcd.caculate_counts(count_map_new, sceneId, diff_type)
                    total_sum_old_map = tcd.caculate_sums(sum_map_old, sceneId, diff_type)
                    total_sum_new_map = tcd.caculate_sums(sum_map_new, sceneId, diff_type)
                    calc_total_sql_map = tcd.make_total_sql((total_count_old_map, total_count_new_map),
                                                            (total_sum_old_map, total_sum_new_map), sceneId, diff_type,
                                                            version)
                    ret, msg = runsqlsOnebyOne(calc_total_sql_map, dbObj, caseId, env_type)

    return ret, msg


def removedata(test_connstr_pair, user_table_name, sceneId='', version='', caseinfo=()):
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    connstr_pairs = test_connstr_pair.split(',')
    old_conn_str, new_conn_str = split_connstr_pair(connstr_pairs)
    olddbObj = DBObj(old_conn_str)
    if old_conn_str != new_conn_str:
        newdbObj = DBObj(new_conn_str)
    else:
        newdbObj = olddbObj
    conditions = ''
    table_name = user_table_name
    tcd = TabClustDef(user_table_name, dbObj)
    tnc = TableNameConvertor(dbObj, projId, taskId, caseId, stepId)
    tcd.setTNC(tnc)
    tcd = tcd.generatedbdefobj(casetype, caseId)
    tcd.redefine_table_define()
    dropsqlmap = tcd.make_drop_table_sql(conditions)
    ret = True
    msg = ''
    ret, msg = runsqlsOnebyOne(dropsqlmap, newdbObj, caseId, TestDBEnvManager.ENV_TYPE_MAP['new'])
    ret, msg = runsqlsOnebyOne(dropsqlmap, olddbObj, caseId, TestDBEnvManager.ENV_TYPE_MAP['old'])

    return ret, msg


def getunionedcolumns(test_connstr_pair, user_table_name, table_sql, caseinfo=()):
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    return getunionedcolumnsByproj(test_connstr_pair, user_table_name, table_sql, dbObj, projId)


def getunionedcolumnsByproj(test_connstr_pair, user_table_name, table_sql, dbObj, projId):
    connstr_pairs = test_connstr_pair.split(',')
    old_conn_str, new_conn_str = split_connstr_pair(connstr_pairs)
    olddbObj = DBObj(old_conn_str)
    if old_conn_str != new_conn_str:
        newdbObj = DBObj(new_conn_str)
    else:
        newdbObj = olddbObj

    tcd = TabClustDef(user_table_name, dbObj)
    dblnk = TestDBEnvManager.getTestDBEnvManager(dbObj, projId).dblnk2.dblnk
    sqls, cols = tcd.getunionedtabcolumns(table_sql, dblnk)
    return cols


def runsqlfunctions(test_connstr_pair, user_table_name, env_type, version='', casetype='-1', caseinfo=()):
    ret = True
    msg = ''
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    sql = "select object_name, object_type,default_owner,object_def from at_sys_obj_define order by seq asc"
    lst, msg = dbObj.execute(sql)
    if len(lst) == 0:
        return True, ''
    if dbObj.executefailed(msg):
        return False, msg
    conn_pair = test_connstr_pair.split(',')

    olddbObj = newdbObj = None
    old_conn_str, new_conn_str = split_connstr_pair(conn_pair)
    olddbObj = DBObj(old_conn_str)
    if old_conn_str != new_conn_str:
        newdbObj = DBObj(new_conn_str)
    else:
        newdbObj = olddbObj
    currdbObj = olddbObj
    old_new_flag = 0
    if env_type == TestDBEnvManager.ENV_TYPE_MAP['new']:
        old_new_flag = 1
    if old_new_flag == 1:
        currdbObj = newdbObj

    for item in lst:
        owner = item[-2]
        definition = item[-1].read()
        sql = definition  # '''begin\nexecute immediate '%s';\nend;\n''' %(definition.replace("'","''"))
        if owner.lower() == 'all' or owner.lower() == 'test':
            ret, msg1 = dbObj.execute(sql)
            if dbObj.executefailed(msg):
                msg += msg1
        if owner.lower() == 'all' or owner.lower() == 'busi':
            ret, msg1 = olddbObj.execute(sql)
            if olddbObj.executefailed(sql):
                msg += msg1
            ret, msg1 = newdbObj.execute(sql)
            if newdbObj.executefailed(sql):
                msg += msg1

    table_name = user_table_name
    tcd = TabClustDef(user_table_name, dbObj)
    tnc = TableNameConvertor(dbObj, projId, taskId, caseId, stepId)
    tcd.setTNC(tnc)
    tcd.setbusiId()
    busiId = tcd.busi_id
    tcd = tcd.generatedbdefobj(casetype, caseId)
    tcd.redefine_table_define()
    tcd.get_last_table_define()

    dbuser = TestDBEnvManager.getTestDBEnvManager(dbObj, projId).getactualdbuser('', old_new_flag, table_name)
    all_region_code_lst = ALL_REGION_CODES
    prints('------------region_code_list:---------------', all_region_code_lst)
    region_codes = StringObj.convertlst2str(all_region_code_lst)
    recent_dates = tcd.tnc.getprobablydays()
    recent_dates_str = StringObj.convertlst2str(recent_dates)
    minus_keys = tcd.minus_item.recs
    used_fields = ''
    if minus_keys == '':
        minus_keys = tcd.key_item.recs + ',' + tcd.diff_item.recs
    dblnk_str_old, dblnk_str_new, dblnk_test_str = get_all_dblnks(dbObj)

    rootTcId = tcd.getroottc(tnc.caseId, dbObj)

    sql0 = '''
declare
    table_name varchar2(32) := '%s';
    dbuser varchar2(32) := '%s';
    region_codes number_array_20 := number_array_20(%s);
    recent_dates number_array_20 := number_array_20(%s);
    minus_keys varchar2(512) := '%s';
    task_id number := %s;
    tc_id number := %s;
    old_new_flag number := %s;
    dblnk_str_old varchar2(32) := '%s';
    dblnk_str_new varchar2(32) := '%s';
    dblnk_test_str varchar2(32) := '%s';
begin
    init_and_clear_last_result(table_name, dbuser, region_codes,recent_dates,
                                minus_keys, task_id, tc_id, old_new_flag,
                                dblnk_str_old,  dblnk_str_new, dblnk_test_str);
end;
        ''' % (table_name, dbuser, region_codes, recent_dates_str, minus_keys, str(tnc.taskId), str(tnc.caseId), \
               str(old_new_flag), dblnk_str_old, dblnk_str_new, dblnk_test_str)
    ret, msg1 = currdbObj.execute("execute immediate '" + sql0.replace("'", "''") + "'")
    if currdbObj.executefailed(msg1):
        msg += msg1
        return ret, msg

    diffscenes = tcd.generate_condstr_forscene('', '')
    difftypes = tcd.generate_condstr_fordifftype('', '')
    for region_code in all_region_code_lst:
        for diff_sceneId in diffscenes.keys():
            diff_sceneConst = diffscenes[diff_sceneId][1]
            diff_sceneCond = diffscenes[diff_sceneId][0]
            diff_sceneDesc = ''
            for diff_type in difftypes.keys():
                diff_cond = difftypes[diff_type]
                table_def = ''
                if str(region_code) in tcd.last_table_define_pair_map.keys():
                    if len(tcd.last_table_define_pair_map[str(region_code)]) > 0:
                        table_def = tcd.last_table_define_pair_map[str(region_code)][0][env_type]
                sql1 = '''
declare
    difftype_info diff_info := diff_info(%s,%s,'%s');
    diff_scene diff_scene_info := diff_scene_info(%s, %s,'%s','%s','%s');
    table_name varchar(32) := '%s';
    region_code number := %s;
    current_date number := %s;
    used_fields varchar2(1024) := '%s';
    minus_keys varchar2(1024) := '%s';
    old_new_flag number(2) := '%s';
    table_def varchar2(4000) := '%s';
    dblnk_str varchar2(16) := '%s';
    dblnk_test_str varchar2(16) := '%s';
    task_id number := %s;
    tc_id number := %s;
begin
    make_diff_sql_with_scene_type2(difftype_info,  diff_scene,table_name, region_code,
                                    current_date,used_fields,minus_keys, old_new_flag,
                                    table_def, dblnk_str, dblnk_test_str,task_id,tc_id );
end;
    ''' % (str(busiId), str(diff_type), diff_cond.replace("'", "''"), str(busiId), str(diff_sceneId),
           diff_sceneCond.replace("'", "''"), \
           diff_sceneConst.replace("'", "''"), diff_sceneDesc.replace("'", "''"), table_name, str(region_code),
           str(current_date), \
           used_fields.replace("'", "''"), minus_keys.replace("'", "''"), table_def.replace("'", "''"), dblnk_str,
           dblnk_test_str, str(tnc.taskId), str(tnc.caseId))

                currdbObj.con.cursor().execute(sql1)
                if currdbObj.executefailed(msg1):
                    msg += msg1
                    return ret, msg
                sql2 = '''
declare
    table_name varchar2(32) := '%s';
    dbuser_old varchar2(32) := '%s';
    dbuser_new varchar2(32) := '%s';
    region_code number := %s;
    current_date number := %s;
    task_id number := %s;
    tc_id number := %s;
    stc_id number := %s;
    old_new_flag number(2) := %s;
    keys varchar2(1024) := '%s';
    diffs varchar2(1024) := '%s';
    diffsums varchar2(1024) := '%s';
    sums varchar2(1024) := '%s';
    counts varchar2(1024) := '%s';
    dblnk_test_str varchar2(32) := '%s';
    dblnk_old_str varchar2(32) := '%s;'
    total_detail_too number := 1; 
begin
    make_diff_sql2(table_name, dbuser_old, dbuser_new, region_code,
                    current_date, task_id, tc_id, stc_id, old_new_flag, 
                    keys, diffs, diffsums, sums, counts, dblnk_test_str,
                    dblnk_old_str, total_detail_too  );
end;
    ''' % (table_name, TestDBEnvManager.getTestDBEnvManager(dbObj, projId).getactualdbuser('', 0, table_name),
           TestDBEnvManager.getTestDBEnvManager(dbObj, projId).getactualdbuser('', 1, table_name), str(region_code),
           str(current_date), \
           str(tnc.taskId), str(rootTcId), str(tnc.caseId), str(old_new_flag), tcd.key_item.recs, tcd.diff_item.recs,
           tcd.diff_numeric_item.recs, \
           tcd.sum_item.recs, tcd.count_item.recs, dblnk_test_str, dblnk_old_str)
                if old_new_flag == 1:
                    ret, msg1 = newdbObj.execute("execute immediate '" + sql2.replace("'", "''") + "'")
                    if newdbObj.executefailed(msg1):
                        msg += msg1
                        return ret, msg1

        sql3 = '''
declare
    table_name varchar2(32) := '%s';
    region_code_str varchar2(512) := '%s';
    task_id number = %s;
    tc_id number = %s;
begin
    update_table_name(table_name, region_code_str, task_id,  tc_id);
end;
    ''' % (table_name, str(region_codes), str(tnc.taskId), str(tnc.caseId))
        if old_new_flag == 1:
            ret, msg1 = newdbObj.execute("execute immediate '" + sql3.replace("'", "''") + "'")
            if newdbObj.executefailed(msg1):
                msg += msg1
                return ret, msg
    if old_new_flag == 0:
        return ret, msg
    for diff_sceneId in diffscenes.keys():
        for diff_type in difftypes.keys():
            sql4 = '''
declare
    table_name varchar2(32) := '%s';
    diff_type number := %s;
    diff_scene_id number := %s;
    region_code number := %s;
    task_id number := %s;
    tc_id number := %s;
begin
    total_sum_info(task_id, tc_id,  table_name, diff_type, diff_scene_id, region_code);
end;
    ''' % (table_name, str(diff_type), str(diff_sceneId), str(region_code), str(tnc.taskId), str(tnc.caseId))
            ret, msg1 = dbObj.execute("execute immediate '" + sql4.replace("'", "''") + "'")
            if dbObj.executefailed(msg1):
                msg += msg1
                return ret, msg1
    sql5 = '''
declare
    table_name varchar2(32) := '%s';
    task_id number := %s;
    tc_id number := %s;
begin
    move_answer(table_name, task_id,  tc_id);
end;
    ''' % (table_name, str(tnc.taskId), str(tnc.caseId))
    ret, msg1 = newdbObj.execute("execute immediate '" + sql5.replace("'", "''") + "'")
    if newdbObj.executefailed(msg1):
        msg += msg1
        return ret, msg
    return ret, msg


def replaceallvar(varmap, orisql):
    newsql = orisql
    for k in varmap.keys():
        newsql = newsql.replace(k, varmap[k])
    return newsql


def runprocbefore2(user_table_name, region_code, env_type, dblnk_str_old, dblnk_str_new, taskId, caseId, rootTcId,
                   current_date, connstr):
    dbObj = DBObj(connstr)
    dbObj.lock2.acquire()
    dbObj.lock.acquire()
    cur = dbObj.con.cursor()
    ret = True
    the_proc_name = 'at_proc_rename_table2'
    msg = cur.var(cx_Oracle.STRING)
    proc_args = []
    proc_args.append(user_table_name)
    proc_args.append(region_code)
    proc_args.append(current_date)
    proc_args.append(env_type)
    proc_args.append(dblnk_str_old)
    proc_args.append(dblnk_str_new)
    proc_args.append(taskId)
    proc_args.append(caseId)
    proc_args.append(rootTcId)
    proc_args.append(msg)
    prints('runprocbefore:', proc_args)
    err_msg_map = {}
    err_msg = ''
    try:
        cur.callproc(the_proc_name, proc_args)
    except Exception as e:
        err_msg = str(e)
        ret = False
        prints('exception of runprocbefore:at_proc_rename_table:', proc_args, str(e))
    finally:
        err_msg = msg.getvalue()
        cur.close()
        dbObj.con.close()
        dbObj.lock.release()
        dbObj.lock2.release()
    if err_msg == None:
        err_msg = ''
    if err_msg != '':
        ret = False
    prints('runprocbefore:', err_msg)
    return ret, err_msg, err_msg_map


def runprocbefore(user_table_name, region_code, env_type, dblnk_str_test, taskId, caseId, rootTcId, current_date,
                  busidbObj_connstr):
    dbObj = DBObj(busidbObj_connstr)
    dbObj.lock2.acquire()
    dbObj.lock.acquire()
    cur = dbObj.con.cursor()
    ret = True
    the_proc_name = 'at_proc_rename_table'
    msg = cur.var(cx_Oracle.STRING)
    proc_args = []
    proc_args.append(user_table_name)
    proc_args.append(region_code)
    proc_args.append(current_date)
    proc_args.append(env_type)
    proc_args.append(dblnk_str_test)
    proc_args.append(taskId)
    proc_args.append(caseId)
    proc_args.append(rootTcId)
    proc_args.append(msg)
    prints('runprocbefore:', proc_args)
    err_msg_map = {}
    err_msg = ''
    try:
        cur.callproc(the_proc_name, proc_args)
    except Exception as e:
        err_msg = str(e)
        ret = False
        prints('exception of runprocbefore:at_proc_rename_table:', proc_args, str(e))
    finally:
        err_msg = msg.getvalue()
        cur.close()
        dbObj.con.close()
        dbObj.lock.release()
        dbObj.lock2.release()
    if err_msg == None:
        err_msg = ''
    if err_msg != '':
        ret = False
    prints('runprocbefore:', err_msg)
    return ret, err_msg, err_msg_map


def runcounteach(user_table_name, region_code, current_date, the_diff_scenes, diffSceneId, env_type, dblnk_str_old,
                 dblnk_str_new, dblnk_str_test, taskId, caseId, rootTcId, busidbObj):
    diffSceneCond = the_diff_scenes[diffSceneId][0]
    diffSceneConst = the_diff_scenes[diffSceneId][1]
    dbObj = DBObj(busidbObj.connstr)
    dbObj.lock2.acquire()
    dbObj.lock.acquire()
    cur = dbObj.con.cursor()
    ret = True
    the_proc_name = 'at_proc_count_each'
    msg = cur.var(cx_Oracle.STRING)
    proc_args = []
    msg = cur.var(cx_Oracle.STRING)
    proc_args = []
    proc_args.append(user_table_name)
    proc_args.append(region_code)
    proc_args.append(current_date)
    proc_args.append(diffSceneId)
    proc_args.append(diffSceneCond)
    proc_args.append(diffSceneConst)
    proc_args.append(env_type)
    proc_args.append(dblnk_str_old)
    proc_args.append(dblnk_str_new)
    proc_args.append(dblnk_str_test)
    proc_args.append(taskId)
    proc_args.append(caseId)
    proc_args.append(rootTcId)
    proc_args.append(msg)
    prints('runcounteach:', proc_args)
    err_msg_map = {}
    try:
        cur.callproc(the_proc_name, proc_args)
    except Exception as e:
        err_msg = str(e)
        ret = False
    finally:
        err_msg = msg.getvalue()
        cur.close()
        dbObj.con.close()
        dbObj.lock.release()
        dbObj.lock2.release()
    if err_msg == None:
        err_msg = ''
    if err_msg != '':
        ret = False
    return ret, err_msg


def runprocafter(user_table_name, region_code, current_date, env_type, dblnk_str_old, dblnk_str_new, dblnk_str_test,
                 taskId, caseId, rootTcId, busidbObj):
    dbObj = DBObj(busidbObj.connstr)
    dbObj.lock2.acquire()
    dbObj.lock.acquire()
    cur = dbObj.con.cursor()
    ret = True
    the_proc_name = 'at_proc_check_total'
    msg = cur.var(cx_Oracle.STRING)
    proc_args = []
    proc_args.append(user_table_name)
    proc_args.append(region_code)
    proc_args.append(current_date)
    proc_args.append(env_type)
    proc_args.append(dblnk_str_old)
    proc_args.append(dblnk_str_new)
    proc_args.append(dblnk_str_test)
    proc_args.append(taskId)
    proc_args.append(caseId)
    proc_args.append(rootTcId)
    proc_args.append(msg)
    prints('runprocafter:', proc_args)
    err_msg_map = {}
    try:
        cur.callproc(the_proc_name, proc_args)
    except Exception as e:
        err_msg = str(e)
        ret = False
    finally:
        err_msg = msg.getvalue()
        cur.close()
        dbObj.con.close()
        dbObj.lock.release()
        dbObj.lock2.release()
    if err_msg == None:
        err_msg = ''
    if err_msg != '':
        ret = False
    return ret, err_msg


def getSpecDay(objname, taskId, rootTcId, dbObj):
    the_recent_dates = []
    if objname.find('$specDay') > -1:
        sql = "select date_str from at_tc_data_prepare_date where task_id = %s and tc_id = %s" % (taskId, rootTcId)
        lst, msg = dbObj.execute(sql)
        if len(lst) > 0:
            date_str = str(lst[0][0])
            the_recent_dates.append(date_str)
            return the_recent_dates
        if the_recent_dates == []:
            the_recent_dates.append(time.strftime('%Y%m%d', time.localtime(time.time())))
    return the_recent_dates


def doProcEachRegionEachTable(user_table_name, proc_name, region_code, the_recent_dates, the_diff_scenes,
                              the_diff_types, err_msg_map, currdbObjstr, dbObjstr, env_type, dblnk_str_old,
                              dblnk_str_new, dblnk_test_str, taskId, caseId, rootTcId):
    currdbObj = DBObj(currdbObjstr)
    dbObj = DBObj(dbObjstr)
    err_msg_map[proc_name][region_code] = {}
    for current_date in the_recent_dates:
        err_msg_map[proc_name][region_code][current_date] = {}
        for diffSceneId in the_diff_scenes.keys():
            if diffSceneId not in err_msg_map[proc_name][region_code][current_date].keys():
                err_msg_map[proc_name][region_code][current_date][diffSceneId] = {}
                err_msg_map[proc_name][region_code][current_date][diffSceneId][11] = ''
                prints('======================')
                try:
                    prints(proc_name, user_table_name, region_code, current_date, the_diff_scenes, diffSceneId,
                           the_diff_types, currdbObj, env_type,
                           err_msg_map[proc_name][region_code][current_date][diffSceneId], dblnk_str_old, dblnk_str_new,
                           dblnk_test_str, taskId, caseId, rootTcId)
                    runProc(proc_name, user_table_name, region_code, current_date, the_diff_scenes, diffSceneId,
                            the_diff_types, 11, currdbObj, env_type,
                            err_msg_map[proc_name][region_code][current_date][diffSceneId], dblnk_str_old,
                            dblnk_str_new, dblnk_test_str, taskId, caseId, rootTcId)
                    prints('------------------')
                except Exception as e:
                    prints('------------' + str(e))
                for diff_type in the_diff_types.keys():
                    if diff_type == '11':
                        continue
                    err_msg_map[proc_name][region_code][current_date][diffSceneId][diff_type] = ''
                    prints(currdbObj.connstr)
                    # t = multiprocessing.Process(target = runProc, args = (proc_name,user_table_name,region_code, current_date,the_diff_scenes,diffSceneId,the_diff_types,diff_type,currdbObj,env_type,err_msg_map[proc_name][region_code][current_date][diffSceneId], dblnk_str_old ,dblnk_str_new ,dblnk_test_str,taskId,caseId,rootTcId))
                    # thread_lst.append(t)
                    prints('runProc:', proc_name, user_table_name, region_code, current_date, the_diff_scenes,
                           diffSceneId, the_diff_types, diff_type, currdbObj, env_type,
                           err_msg_map[proc_name][region_code][current_date][diffSceneId], dblnk_str_old, dblnk_str_new,
                           dblnk_test_str, taskId, caseId, rootTcId)
                    runProc(proc_name, user_table_name, region_code, current_date, the_diff_scenes, diffSceneId,
                            the_diff_types, diff_type, currdbObj, env_type,
                            err_msg_map[proc_name][region_code][current_date][diffSceneId], dblnk_str_old,
                            dblnk_str_new, dblnk_test_str, taskId, caseId, rootTcId)
                    prints('runcounteach:', user_table_name, region_code, current_date, the_diff_scenes, diffSceneId,
                           env_type, dblnk_str_old, dblnk_str_new, dblnk_test_str, taskId, caseId, rootTcId,
                           currdbObj.connstr)
                ret1, msg1 = runcounteach(user_table_name, region_code, current_date, the_diff_scenes, diffSceneId,
                                          env_type, dblnk_str_old, dblnk_str_new, dblnk_test_str, taskId, caseId,
                                          rootTcId, currdbObj)
                if 999 not in err_msg_map[proc_name][region_code][current_date][diffSceneId].keys():
                    err_msg_map[proc_name][region_code][current_date][diffSceneId][999] = ''
                err_msg_map[proc_name][region_code][current_date][diffSceneId][999] += msg1
    pass


def runsqlprocedures(test_connstr_pair, user_table_name, env_type, proc_names, version='', casetype='-1', caseinfo=()):
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    old_conn_str, new_conn_str = test_connstr_pair.split(',')
    table=CmpTableObj(user_table_name)
    cmp_tables=table.build_table()
    for cmp_table in cmp_tables:
        if cmp_table:
            compareobj=Compare(cmp_table)
            compareobj.run()
    ret,msg=(True,'')
    return ret, msg


def runProc(proc_name, table_name, region_code, current_date, the_diff_scenes, diffSceneId, the_diff_types, diff_type,
            busidbObj, env_type, err_msg_map, dblnk_str_old, dblnk_str_new, dblnk_test_str, taskId, caseId, rootTcId):
    # format of the procedure param: thetable,regionCode,theDay,diffScene,diffSceneCond,diffSceneConstCond,difftype,difftypeCond,taskId,caseId,rootTcId,errmsg out
    diffSceneCond = the_diff_scenes[diffSceneId][0]
    diffSceneConst = the_diff_scenes[diffSceneId][1]
    difftypeCond = ''
    if str(diff_type) != '11':
        difftypeCond = the_diff_types[diff_type]
    prints('runProc Started....', busidbObj, busidbObj.connstr)
    dbObj = DBObj(busidbObj.connstr)
    dbObj.lock2.acquire()
    dbObj.lock.acquire()

    prints("runProc:getLOck")
    cur = dbObj.con.cursor()
    ret = True
    the_proc_name = proc_name[:proc_name.find('(')]
    prints(the_proc_name)
    msg = cur.var(cx_Oracle.STRING)
    proc_args = []
    proc_args.append(table_name)
    proc_args.append(region_code)
    proc_args.append(current_date)
    proc_args.append(diffSceneId)
    proc_args.append(the_diff_scenes[diffSceneId][0])
    proc_args.append(the_diff_scenes[diffSceneId][1])
    proc_args.append(diff_type)
    proc_args.append(difftypeCond)
    proc_args.append(env_type)
    proc_args.append(dblnk_str_old)
    proc_args.append(dblnk_str_new)
    proc_args.append(dblnk_test_str)
    proc_args.append(taskId)
    proc_args.append(caseId)
    proc_args.append(rootTcId)
    proc_args.append(msg)
    prints('runProc:proc_args:tablename:1,reg:2,date:3,scene:4-6,difftype:78,env:9,dblnk:10-12,taskinfo:13-15,msg:16',
           proc_args, time.time())
    try:
        cur.callproc(the_proc_name, proc_args)
    except Exception as e:
        err_msg_map[diff_type] = str(e)
        prints('runProc:exception:', str(e))
        ret = False
    finally:
        prints('runProc:end', proc_args, time.time())
        if diff_type not in err_msg_map.keys() or err_msg_map[diff_type] == '':
            err_msg_map[diff_type] = msg.getvalue()
        cur.close()
        dbObj.con.close()
        dbObj.lock.release()
        dbObj.lock2.release()
    if err_msg_map[diff_type] == None:
        err_msg_map[diff_type] = ''
    if err_msg_map[diff_type] != '':
        ret = False
    prints(ret, diff_type, err_msg_map)
    return ret, err_msg_map[diff_type]


def runsqlfiles(test_connstr_pair, user_table_name, env_type, file_names, version='', casetype='-1', caseinfo=()):
    filelst = file_names.split(';')
    sqllst = []
    for it in filelst:
        fp = open(it, 'r')
        if fp == None:
            continue
        lines = fp.readlines()
        fp.close()
        strSql = ''
        for line in lines:
            strSql += line + '\n'
        sqllst.append(strSql)
    return runsqlscripts(test_connstr_pair, user_table_name, env_type, sqllst, version, casetype, caseinfo)


def runsqlscripts(test_connstr_pair, user_table_name, env_type, sqllst, version, casetype, caseinfo):
    ret = True
    msg = ''
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    conn_pair = test_connstr_pair.split(',')

    olddbObj = newdbObj = None
    old_conn_str, new_conn_str = split_connstr_pair(conn_pair)
    olddbObj = DBObj(old_conn_str)
    if old_conn_str != new_conn_str:
        newdbObj = DBObj(new_conn_str)
    else:
        newdbObj = olddbObj
    currdbObj = olddbObj
    old_new_flag = 0
    if env_type == TestDBEnvManager.ENV_TYPE_MAP['new']:
        old_new_flag = 1
    if old_new_flag == 1:
        currdbObj = newdbObj

    tcd = TabClustDef(user_table_name, dbObj)
    tnc = TableNameConvertor(dbObj, projId, taskId, caseId, stepId)
    tcd.setTNC(tnc)
    tcd.setbusiId()
    busiId = tcd.busi_id
    diffscenes = tcd.generate_condstr_forscene('', '')
    difftypes = tcd.generate_condstr_fordifftype('', '')

    dbuser = TestDBEnvManager.getTestDBEnvManager(dbObj, projId).getactualdbuser('', old_new_flag, table_name)
    all_region_code_lst = ALL_REGION_CODES
    region_codes = [-1]
    if tcd.table_define.find('$regionCode') > -1:
        region_codes = StringObj.convertlst2str(all_region_code_lst)
    recent_dates = tcd.tnc.getprobablydays()
    recent_dates_str = StringObj.convertlst2str(recent_dates)

    dblnk_str_old, dblnk_str_new, dblnk_test_str = get_all_dblnks(dbObj)

    rootTcId = tcd.getroottc(tnc.caseId, dbObj)

    thread_lst = []
    err_msg_map = {}
    the_region_codes = region_codes
    the_recent_dates = recent_dates
    the_diff_scenes = diffscenes
    the_diff_types = difftypes
    i = 0
    for sqlscript in sqllst:
        thread_lst = []
        err_msg_map[i] = {}
        if sqlscript.find('$regionCode') == -1:
            the_region_codes = [-1]
        if sqlscript.find('$recentDay') == -1 and sqlscript.find('$currentDay') == -1:
            the_recent_dates = [-1]
        if sqlscript.find('$diffSceneId') == -1:
            the_diff_scenes = {'-1': ['', '']}
        if sqlscript.find('$diffTypeId') == -1:
            the_diff_types = {'-1': ''}
        for region_code in the_region_codes:
            err_msg_map[i][region_code] = {}
            for cuurent_date in the_recent_dates:
                err_msg_map[i][region_code][current_date] = {}
                for diffSceneId in the_diff_scenes.keys():
                    err_msg_map[i][region_code][current_date][diffSceneId] = {}
                    for diff_type in the_diff_types.keys():
                        err_msg_map[i][region_code][current_date][diffSceneId][diff_type] = ''
                        t = multiprocessing.Process(target=runSqlScript, args=(
                        sqlscript, user_table_name, region_code, current_date, the_diff_scenes, diffSceneId,
                        the_diff_types, diff_type, env_type, currdbObj,
                        err_msg_map[i][region_code][current_date][diffSceneId], dblnk_str_old, dblnk_str_new,
                        dblnk_test_str, tnc.taskId, tnc.caseId, rootTcId))
                        thread_lst.append(t)
        for t in thread_lst:
            t.start()
        waiting4threads(5, thread_lst, caseId, dbObj)
        i += 1
    ret = True
    for i in err_msg_map.keys():
        sqlscript = sqllst[i]
        for region_code in err_msg_map[i].keys():
            for current_date in err_msg_map[i][region_code].keys():
                for diffSceneId in err_msg_map[i][region_code][current_date].keys():
                    cond1 = generatecond(the_diff_scenes[diffSceneId][0])
                    cond2 = generatecond(the_diff_scenes[diffSceneId][1])
                    for diff_type in err_msg_map[i][region_code][current_date][diffSceneId].keys():
                        cond3 = generatecond(the_diff_types[diff_type])
                        vm = {'$regionCode': str(region_code),
                              '$currentDay': str(current_date), '$recentDay': str(current_date),
                              '$diffSceneId': str(diffSceneId), '$diffTypeId': str(diff_type),
                              '$diffSceneCond': cond1, '$diffSceneConstCond': cond2, '$diffTypeCond': cond3}
                        sqlscript = replaceallvar(vm, sqlscript)
                        if err_msg_map[i][region_code][current_date][diffSceneId][diff_type] != '':
                            ret = False
                            msg += sqlscript + ':' + err_msg_map[i][region_code][current_date][diffSceneId][
                                diff_type] + ';;'
    return ret, msg


def generatecond(cond):
    if cond.strip() == '':
        return ''
    return ' and (' + cond + ')'


def runSqlScript(sqlscript, user_table_name, region_code, current_date, the_diff_scenes, diffSceneId, the_diff_types,
                 diff_type, env_type, currdbObj, dblnk_str_old, dblnk_str_new, dblnk_test_str, taskId, caseId,
                 rootTcId):
    ret = True
    msg = ''
    diffSceneCond = the_diff_scenes[diffSceneId][0]
    diffSceneConst = the_diff_scenes[diffSceneId][1]
    difftypeCond = the_diff_types[diff_type]
    dbObj = DBObj(busidbObj.connstr)
    sqlscript1 = sqlscript
    sqlscript1 = sqlscript1.replace('$tableName', user_table_name)
    sqlscript1 = sqlscript1.replace('$regionCode', str(region_code))
    sqlscript1 = sqlscript1.replace('$currentDay', str(current_date))
    sqlscript1 = sqlscript1.replace('$recentDay', str(current_date))
    sqlscript1 = sqlscript1.replace('$diffSceneId', str(diffSceneId))
    sqlscript1 = sqlscript1.replace('$diffSceneCond', generatecond(the_diff_scenes[diffSceneId][0]))
    sqlscript1 = sqlscript1.replace('$diffSceneConstCond', generatecond(the_diff_scenes[diffSceneId][1]))
    sqlscript1 = sqlscript1.replace('$diffTypeId', str(diff_type))
    sqlscript1 = sqlscript1.replace('$diffTypeCond', generatecond(the_diff_types[diff_type]))
    sqlscript1 = sqlscript1.replace('$envType', str(env_type))
    sqlscript1 = sqlscript1.replace('$olddblnk', dblnk_str_old)
    sqlscript1 = sqlscript1.replace('$newdblnk', dblnk_str_new)
    sqlscript1 = sqlscript1.replace('$testdblnk', dblnk_test_str)
    sqlscript1 = sqlscript1.replace('$currTaskId', str(taskId))
    sqlscript1 = sqlscript1.replace('$currTcId', str(caseId))
    sqlscript1 = sqlscript1.replace('$currRootTcId', str(rootTcId))
    ret, msg = dbObj.execute("execute immediate '" + sqlscript1.replace("'", "''") + "'")

    return ret, msg


def getenvvalue(dbObj, env_name, env_type, projId=-1):
    sql = "select env_value from at_tc_sysenv where lower(env_name)='%s' and old_new_system = %s and proj_Id = %s" % (
    env_name.lower(), env_type, projId)
    lst, msg = dbObj.execute(sql)
    env_info = []
    for it in lst:
        env_info.append(it[0])
    prints('getenvvalue:', env_info)
    if str(type(env_info)).find('list') > -1:
        if env_info != [] and env_info != None and len(env_info) > 0:
            return env_info[0]
    else:
        return env_info
    if projId != -1:
        env_info = getenvvalue(dbObj, env_name, env_type, -1)
        prints('getenvvalue:', env_info)
    if str(type(env_info)).find('list') > -1 and env_info != [] and env_info != None and len(env_info) > 0:
        return env_info[0]
    else:
        return env_info
    return ''


def gettotals(caseId, dbObj):
    sql = "select busi_id,old_new_env from at_tc where tc_id = '%s' " % (str(caseId))
    lst, msg = dbObj.execute(sql)
    if len(lst) == 0 or len(lst[0]) == 0 or lst[0][0] == None:
        return 0, 0, 0
    busi_id = lst[0][0]
    old_new_flag = lst[0][1]
    sql = "select count(1) from at_tc_compared_table_selected where stc_id = %s "
    lst, msg = dbObj.execute(sql)
    if len(lst) == 0 or len(lst[0]) == 0 or lst[0][0] == None:
        return busi_id, old_new_flag, 0
    return busi_id, old_new_flag, lst[0][0]


def getfinished(taskId, caseId, dbObj):
    sql = "select busi_id,root_tc_id,old_new_env from at_tc where tc_id = '%s' " % (str(caseId))
    lst, msg = dbObj.execute(sql)
    if len(lst) == 0 or len(lst[0]) == 0 or lst[0][0] == None:
        return 0, 0, 0, 0
    busi_id = lst[0][0]
    root_tc_id = lst[0][1]
    old_new_env = lst[0][2]
    sql = "select total,finished from at_tc_process_result  where task_id = '%s' and tc_id = '%s' and busi_id = '%s' and old_new_flag = '%s' and index_value = 3" % (
    taskId, root_tc_id, busi_id, old_new_env)
    lst, msg = dbObj.execute(sql)
    if len(lst) == 0 or len(lst[0]) == 0 or lst[0][0] == None:
        return busi_id, root_tc_id, 0, 0
    total = lst[0][0]
    finished = lst[0][1]
    return busi_id, root_tc_id, total, finished
    # task_id,tc_id,busi_id,old_new_flag,index_value,total,finished


def dbcompare_dz(env_type, caseinfo=()):
    printfunc(get_current_func_name4log())
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    start_time = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    sql = '''insert into at_tc_dz_time (TASK_ID, INDEX_ID, START_TIME, END_TIME, STATUS, NOTE, PROJ_ID,OLD_NEW_ENV,BUSI_ID)
    values (%s, 6, to_date('%s','YYYY-mm-dd hh24:MI:SS'),'', 0, '', %s,%s,-1);
        ''' % (taskId, start_time, projId, env_type)
    print sql
    dbObj.execute(sql)
    conn1 = getenvvalue(dbObj, 'dbconn_old', 0, projId)
    conn2 = getenvvalue(dbObj, 'dbconn_new', 1, projId)
    prints(get_current_func_name4log(), 'dbconn_old=', conn1, 'dbconn_new=', conn2)
    test_connstr_pair = conn1 + ',' + conn2
    busiconn = conn1
    if env_type == '1':
        busiconn = conn2
    savespeed(dbObj, taskId, busiconn, caseId, env_type)
    # busi_id,old_new_flag,total = gettotals(caseId,dbObj)
    # p = ProcessInfo(taskId,dbObj)
    # p.updatetotal(total,busi_id,env_type)
    ret = dbcompare_dz2(test_connstr_pair, env_type, False, caseinfo)
    prints(ret)
    printfunc(get_current_func_name4log(), 1)
    end_time = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    update_sql = '''update at_tc_dz_time set end_time=to_date('%s','YYYY-mm-dd hh24:MI:SS'), status=2 where task_id =%s and proj_id=%s and index_id=6 and old_new_env=%s''' % (
        end_time, taskId, projId, env_type)
    dbObj.execute(update_sql)
    print update_sql
    return ret


def deleteall(caseId, taskId, owner, dbObj):
    tnames = []
    rootTcId = -1
    sql = "select root_tc_id from at_tc where tc_id = %s" % (caseId)
    lst, msg = dbObj.execute(sql)
    if len(lst) > 0 and len(lst[0]) > 0:
        rootTcId = lst[0][0]
    if rootTcId == -1:
        return
    sql = "delete from at.at_tc_dbcmp_diff_detail where task_id != '%s'" % (taskId)
    lst, msg = dbObj.execute(sql)
    sql = "delete from at.at_tc_dbcmp_diff_stat1 where task_id != '%s'" % (taskId)
    lst, msg = dbObj.execute(sql)
    sql = "select table_name from at_tc_compared_table_selected where tc_id = %s" % (rootTcId)
    lst0, msg = dbObj.execute(sql)
    flag = ''
    if owner.lower() == 'ud_old':
        flag = 'o'
    else:
        flag = 'n'
    for it in lst0:
        table_name = it[0]
        sql = "select 'truncate table '||owner||'.'||table_name from all_all_tables where lower(owner) = '%s' and lower(table_name) like '%s%%' and lower(table_name) not like '%s%%\_%s\_%%' escape '\\'" % (
        owner.lower(), table_name.lower().strip(), table_name.lower().strip(), rootTcId)
        sql2 = "select 'truncate table '||owner||'.'||table_name from all_all_tables where lower(owner) = '%s' and lower(table_name) like 'df%s_%%_%s%%'" % (
        owner.lower(), flag, table_name.lower())
        lst, msg = dbObj.execute(sql)
        lst1, msg1 = dbObj.execute(sql2)
        for item in lst:
            # truncatesql,dropsql=item.split(';')
            truncatesql = item[0]
            prints('delete all:', truncatesql)
            # dbObj.execute(truncatesql)
            # dbObj.execute(dropsql)
        for item in lst1:
            tsql = item[0]
            prints('delete all:', tsql)
            dbObj.execute(tsql)


def dbcompare_dz2(test_connstr_pair, env_type, parrallel=False, caseinfo=()):
    printfunc(get_current_func_name4log())
    casetype = '-1'
    version = '0'
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    sql = "select runtype from at_tc_task where task_id = %s" % (taskId)
    lst, msg = dbObj.execute(sql)
    runtype = '1'
    if len(lst) > 0 and len(lst[0]) > 0:
        runtype = str(lst[0][0])
    if env_type == str(TestDBEnvManager.ENV_TYPE_MAP['new']):
        ret = sureOldEnvRunOver(taskId, caseId, dbObj)
        if runtype != '4':
            deleteall(caseId, taskId, 'ud_new', dbObj)
    else:
        if runtype != '4':
            deleteall(caseId, taskId, 'ud_old', dbObj)
    sql = "select table_name,type from at_tc_compared_table_selected where stc_id = %s and lower(nvl(owner,' ')) not like '%%mdb%%' and lower(nvl(table_name,' ')) not like '%%err%%'" % (
    str(caseId))
    lst, msg = dbObj.execute(sql)
    procLst = []
    for item in lst:
        user_table_name = item[0]
        compare_type = str(item[1])
        p = None
        if compare_type == BUSI_COMPARE_TYPE_MAP['Auto']:
            if parrallel:
                p = multiprocessing.Process(target=dbcompare4envs, args=(
                test_connstr_pair, user_table_name, env_type, version, casetype, caseinfo))
            else:
                dbcompare4envs(test_connstr_pair, user_table_name, env_type, version, casetype, caseinfo)
        elif compare_type == BUSI_COMPARE_TYPE_MAP['File']:
            sql = "select sql_path from at_tc_compared_table_selected where stc_id = %s and lower(table_name) = '%s' and type = %s" % (
            str(caseId), user_table_name.lower(), BUSI_COMPARE_TYPE_MAP['File'])
            lst0, msg = dbObj.execute(sql)
            file_names = ""
            if len(lst0) > 0 and lst0[0] != [] and lst0[0][0] != None:
                file_names = lst0[0][0]
                if parrallel:
                    p = multiprocessing.Process(target=runsqlfiles, args=(
                    test_connstr_pair, user_table_name, env_type, file_names, version, casetype, caseinfo))
                else:
                    runsqlfiles(test_connstr_pair, user_table_name, env_type, file_names, version, casetype, caseinfo)
        elif compare_type == BUSI_COMPARE_TYPE_MAP['SqlScript']:
            sql = "select sql from at_tc_compared_table_selected where stc_id = %s and lower(table_name) = '%s' and type = %s" % (
            str(caseId), user_table_name.lower(), BUSI_COMPARE_TYPE_MAP['SqlScript'])
            lst0, msg = dbObj.execute(sql)
            sqlscripts = ""
            if len(lst0) > 0 and lst0[0] != [] and lst0[0][0] != None:
                sqlscripts = lst0[0][0]
                sqllst = sqlscripts.split(';')
                if parrallel:
                    p = multiprocessing.Process(target=runsqlscripts, args=(
                    test_connstr_pair, user_table_name, env_type, sqllst, version, casetype, caseinfo))
                else:
                    runsqlscripts(test_connstr_pair, user_table_name, env_type, sqllst, version, casetype, caseinfo)
        elif compare_type == BUSI_COMPARE_TYPE_MAP['Procedure']:
            sql = "select procedure_name from at_tc_compared_table_selected where stc_id = %s and lower(table_name) = '%s' and type = %s" % (
            str(caseId), user_table_name.lower(), BUSI_COMPARE_TYPE_MAP['Procedure'])
            lst0, msg = dbObj.execute(sql)

            if len(lst0) > 0 and lst0[0] != [] and lst0[0][0] != None:
                proc_names = lst0[0][0]
                if parrallel:
                    p = multiprocessing.Process(target=runsqlprocedures, args=(
                    test_connstr_pair, user_table_name, env_type, proc_names, version, casetype, caseinfo))
                else:
                    runsqlprocedures(test_connstr_pair, user_table_name, env_type, proc_names, version, casetype,
                                     caseinfo)
        if p != None:
            procLst.append(p)
            p.start()
    waiting4threads(10, procLst, caseId, dbObj)
    printfunc(get_current_func_name4log(), 1)

    return True, ''


def dbcompare_dz_err(env_type, caseinfo=()):
    printfunc(get_current_func_name4log())
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    start_time = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    sql = '''insert into at_tc_dz_time (TASK_ID, INDEX_ID, START_TIME, END_TIME, STATUS, NOTE, PROJ_ID,OLD_NEW_ENV,BUSI_ID)
values (%s, 7, to_date('%s','YYYY-mm-dd hh24:MI:SS'),'', 0, '', %s,%s,-1);''' % (taskId, start_time, projId, env_type)
    print sql
    dbObj.execute(sql)
    conn1 = getenvvalue(dbObj, 'dbconn_old', 0, projId)
    conn2 = getenvvalue(dbObj, 'dbconn_new', 1, projId)
    test_connstr_pair = conn1 + ',' + conn2
    ret = dbcompare_dz_err2(test_connstr_pair, env_type, False, caseinfo)
    prints(ret)
    printfunc(get_current_func_name4log(), 1)
    end_time = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    update_sql = '''update at_tc_dz_time set end_time=to_date('%s','YYYY-mm-dd hh24:MI:SS'), status=2 where task_id =%s and proj_id=%s and index_id=7 and old_new_env=%s''' % (
        end_time, taskId, projId, env_type)
    dbObj.execute(update_sql)
    print update_sql
    return ret


def dbcompare_dz_err2(test_connstr_pair, env_type, parrallel=False, caseinfo=()):
    printfunc(get_current_func_name4log())
    casetype = '-1'
    version = '0'
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)

    if env_type == str(TestDBEnvManager.ENV_TYPE_MAP['new']):
        ret = sureOldEnvRunOver(taskId, caseId, dbObj)
    sql = "select table_name,type from at_tc_compared_table_selected where stc_id = %s and lower(nvl(owner,' ')) not like '%%mdb%%' and lower(table_name) like '%%err%%'" % (
    str(caseId))
    lst, msg = dbObj.execute(sql)
    procLst = []
    for item in lst:
        user_table_name = item[0]
        compare_type = str(item[1])
        p = None
        if compare_type == BUSI_COMPARE_TYPE_MAP['Auto']:
            if not parrallel:
                dbcompare4envs(test_connstr_pair, user_table_name, env_type, version, casetype, caseinfo)
            else:
                p = multiprocessing.Process(target=dbcompare4envs, args=(
                test_connstr_pair, user_table_name, env_type, version, casetype, caseinfo))
        elif compare_type == BUSI_COMPARE_TYPE_MAP['File']:
            if not parrallel:
                runsqlfiles(test_connstr_pair, user_table_name, env_type, file_names, version, casetype, caseinfo)
            else:
                p = multiprocessing.Process(target=runsqlfiles, args=(
                test_connstr_pair, user_table_name, env_type, file_names, version, casetype, caseinfo))
        elif compare_type == BUSI_COMPARE_TYPE_MAP['SqlScript']:
            if not parrallel:
                runsqlscripts(test_connstr_pair, user_table_name, env_type, sqllst, version, casetype, caseinfo)
            else:
                p = multiprocessing.Process(target=runsqlscripts, args=(
                test_connstr_pair, user_table_name, env_type, sqllst, version, casetype, caseinfo))
        elif compare_type == BUSI_COMPARE_TYPE_MAP['Procedure']:
            sql = "select procedure_name from at_tc_compared_table_selected where stc_id = %s and lower(table_name) = '%s' and type = %s" % (
            str(caseId), user_table_name.lower(), BUSI_COMPARE_TYPE_MAP['Procedure'])
            lst0, msg = dbObj.execute(sql)
            proc_names = ""
            if len(lst0) > 0 and lst0[0] != [] and lst0[0][0] != None:
                proc_names = lst0[0][0]
                prints(proc_names)
                if not parrallel:
                    runsqlprocedures(test_connstr_pair, user_table_name, env_type, proc_names, version, casetype,
                                     caseinfo)
                else:
                    p = multiprocessing.Process(target=runsqlprocedures, args=(
                    test_connstr_pair, user_table_name, env_type, proc_names, version, casetype, caseinfo))
                    if p != None:
                        procLst.append(p)
                        p.start()
    waiting4threads(10, procLst, caseId, dbObj)
    printfunc(get_current_func_name4log(), 1)
    return True, ''


def mdbcompare_dz(env_type, caseinfo=()):
    printfunc(get_current_func_name4log())
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    start_time = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    sql = '''insert into at_tc_dz_time (TASK_ID, INDEX_ID, START_TIME, END_TIME, STATUS, NOTE, PROJ_ID,OLD_NEW_ENV,BUSI_ID)
values (%s, 8, to_date('%s','YYYY-mm-dd hh24:MI:SS'),'', 0, '', %s,%s,-1);''' % (taskId, start_time, projId, env_type)
    print sql
    dbObj.execute(sql)
    conn1 = getenvvalue(dbObj, 'dbconn_old', 0, projId)
    conn2 = getenvvalue(dbObj, 'dbconn_new', 1, projId)
    test_connstr_pair = conn1 + ',' + conn2
    # busi_id,old_new_flag,total = gettotals(caseId,dbObj)
    # p = ProcessInfo(taskId,dbObj)
    # p.updatetotal(total,busi_id,env_type)
    ret = mdbcompare_dz2(test_connstr_pair, env_type, False, caseinfo)
    printfunc(get_current_func_name4log(), 1)
    prints(ret)
    sql = '''update at_tc_result_total set remained_time=0,rate=100 where task_id =%s''' % (taskId)
    lst, msg = dbObj.execute(sql)
    end_time = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    update_sql = '''update at_tc_dz_time set end_time=to_date('%s','YYYY-mm-dd hh24:MI:SS'), status=2 where task_id =%s and proj_id=%s and index_id=8 and old_new_env=%s''' % (
        end_time, taskId, projId, env_type)
    dbObj.execute(update_sql)
    print update_sql
    return ret


def mdbcompare_dz2(test_connstr_pair, env_type, parrallel=False, caseinfo=()):
    printfunc(get_current_func_name4log())
    casetype = '-1'
    version = '0'
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)

    if env_type == str(TestDBEnvManager.ENV_TYPE_MAP['new']):
        ret = sureOldEnvRunOver(taskId, caseId, dbObj)
    sql = "select table_name,type from at_tc_compared_table_selected where stc_id = %s and ( lower(nvl(owner,' ')) like '%%mdb%%' or lower(nvl(owner,' ')) like '%%notif%%' )" % (
    str(caseId))
    lst, msg = dbObj.execute(sql)
    procLst = []
    for item in lst:
        p = None
        user_table_name = item[0]
        compare_type = str(item[1])
        if compare_type == BUSI_COMPARE_TYPE_MAP['Auto']:
            if not parrallel:
                dbcompare4envs(test_connstr_pair, user_table_name, env_type, version, casetype, caseinfo)
            else:
                p = multiprocessing.Process(target=dbcompare4envs, args=(
                test_connstr_pair, user_table_name, env_type, version, casetype, caseinfo))
        elif compare_type == BUSI_COMPARE_TYPE_MAP['File']:
            if not parrallel:
                runsqlfiles(test_connstr_pair, user_table_name, env_type, file_names, version, casetype, caseinfo)
            else:
                p = multiprocessing.Process(target=runsqlfiles, args=(
                test_connstr_pair, user_table_name, env_type, file_names, version, casetype, caseinfo))
        elif compare_type == BUSI_COMPARE_TYPE_MAP['SqlScript']:
            if not parrallel:
                runsqlscripts(test_connstr_pair, user_table_name, env_type, sqllst, version, casetype, caseinfo)
            else:
                p = multiprocessing.Process(target=runsqlscripts, args=(
                test_connstr_pair, user_table_name, env_type, sqllst, version, casetype, caseinfo))
        elif compare_type == BUSI_COMPARE_TYPE_MAP['Procedure']:
            sql = "select procedure_name from at_tc_compared_table_selected where stc_id = %s and lower(table_name) = '%s' and type = %s" % (
            str(caseId), user_table_name.lower(), BUSI_COMPARE_TYPE_MAP['Procedure'])
            lst0, msg = dbObj.execute(sql)
            proc_names = ""
            if len(lst0) > 0 and lst0[0] != [] and lst0[0][0] != None:
                proc_names = lst0[0][0]
                prints(proc_names)
                if not parrallel:
                    runsqlprocedures(test_connstr_pair, user_table_name, env_type, proc_names, version, casetype,
                                     caseinfo)
                else:
                    p = multiprocessing.Process(target=runsqlprocedures, args=(
                    test_connstr_pair, user_table_name, env_type, proc_names, version, casetype, caseinfo))
                    if p != None:
                        procLst.append(p)
                        p.start()
    waiting4threads(10, procLst, caseId, dbObj)
    printfunc(get_current_func_name4log(), 1)

    return True, ''


def exportMdbTable(mdb_ip, mdb_port, tabname, dbconn, cfgpath):
    tmpfile = ''
    strpath = os.path.dirname(cfgpath)
    tmpfile = os.path.join(strpath, 'atp_mdb_%s_%s.sql' % (str(mdb_ip).replace('.', '_'), str(mdb_port)))
    try:
        f = open(tmpfile, 'w')
        mdb_export_cmd = 'export'

        if tabname.rfind('_') != -1:
            f.write('%s %s %s %s 10000;\n' % (mdb_export_cmd, tabname[:tabname.rfind('_')], tabname, dbconn))
        else:
            f.write('%s %s %s %s 10000;\n' % (mdb_export_cmd, tabname, tabname, dbconn))
        f.write('exit;\n')
        f.close()
    except IOError as e:
        return False, 'failed to write tmpfile:%s' % tmpfile
    strcmd = 'mdb_client %s %s < %s' % (mdb_ip, mdb_port, tmpfile)
    p = subprocess.Popen(strcmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderrdata = p.communicate()
    return True, ''


def mdb2db(env_type, caseinfo=()):
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)
    strConn = dbObj.connstr
    sql = "select lower(para_name),para_value from at_sys_base_para where lower(para_name) in ('dbuser_old','dbuser_new','dbuser_test')"
    lst, msg = dbObj.execute(sql)
    dbuser_test = strConn[:strConn.find('/')]
    dbuser_old = dbuser_test
    dbuser_new = dbuser_test
    if len(lst) > 0 and len(lst[0]) > 0:
        for item in lst:
            if item[0] == 'dbuser_test':
                dbuser_test = item[1]
            if item[0] == 'dbuser_old':
                dbuser_old = item[1]
            if item[0] == 'dbuser_new':
                dbuser_new = item[1]
    if env_type == str(TestDBEnvManager.ENV_TYPE_MAP['old']):
        strConn = strConn.replace(dbuser_test + '/' + dbuser_test, dbuser_old + '/' + dbuser_old)
    if env_type == str(TestDBEnvManager.ENV_TYPE_MAP['new']):
        strConn = strConn.replace(dbuser_test + '/' + dbuser_test, dbuser_new + '/' + dbuser_new)

    sql = "select distinct(lower(owner)) from at_tc_compared_table_selected where stc_id = %s  and lower(owner) like '%%mdb%%' " % (
    str(caseId))
    lst, msg = dbObj.execute(sql)
    ret = True
    for it in lst:
        mdb_name = it[0]
        ret, msg = mdb2db2(strConn, env_type, mdb_name, mdb_name + '.cfg', False, caseinfo)
        if ret == False:
            return ret, msg
    return True, ''


def mdb2db2(strConn, env_type, mdb_name, mdbcfgfile, parrallel=False, caseinfo=()):
    taskId, caseId, stepId, dbObj, hsyslog, sceneobj, projId = analysecaseinfo(caseinfo)

    mdb_info = ''
    mdb_info = getenvvalue(dbObj, mdb_name.lower(), env_type, projId)
    prints('mdb2db', mdb_info)
    prints('mdb2db', mdb_info, 'mdbname:', mdb_name, 'env_type:', env_type)
    if mdb_info.strip() == '':
        return False, 'mdb_info is empty' + mdb_name + str(env_type)
    mdb_ip, mdb_port = mdb_info.split(':')
    mdb_port = int(mdb_port)
    prints(mdb_ip, mdb_port)
    cfgpath = getenvvalue(dbObj, mdb_name.lower().strip() + '_cfgpath', env_type, projId)
    host_info_name = mdb_name.lower()
    if host_info_name[-1].isdigit():
        host_info_name = host_info_name[:-1]
    host_info = getenvvalue(dbObj, host_info_name.lower() + '_host', env_type, projId)
    if str(type(cfgpath)).find('list') > -1:
        cfgpath = cfgpath[0]
    if str(type(host_info)).find('list') > -1:
        host_info = host_info[0]
    idx00 = host_info.find('/')
    idx01 = host_info.rfind('@')
    user = host_info[:idx00]
    pwd = host_info[idx00 + 1:idx01]
    hostip = host_info[idx01 + 1:]
    sql = "select table_name,type from at_tc_compared_table_selected where stc_id = %s and lower(owner) like '%%%s%%'" % (
    str(caseId), mdb_name.lower())
    lst, msg = dbObj.execute(sql)
    procLst = []

    for it in lst:
        tablename = it[0]
        tcd = TabClustDef(tablename, dbObj)
        tnc = TableNameConvertor(dbObj, projId, taskId, caseId, stepId)
        tcd.setTNC(tnc)
        tcd.setbusiId()
        sql = "select procedure_name from at_tc_compared_table_selected where stc_id = %s and lower(table_name) = '%s' and type = %s" % (
        str(caseId), tablename.lower(), BUSI_COMPARE_TYPE_MAP['Procedure'])
        lstm, msg = dbObj.execute(sql)
        procname = ''
        if len(lstm) > 0:
            procname = lstm[0][0]
        rootTcId = tcd.getroottc(caseId, dbObj)
        # suffix = time.strftime('%Y%m%d',time.localtime(time.time()))
        specdays = getSpecDay(procname, taskId, rootTcId, dbObj)
        if len(specdays) == 0:
            recent_dates = tcd.tnc.getprobablydays()
            recent_dates = cuteach(recent_dates, 2)
            specdays = recent_dates
            sday = specdays[0]
        else:
            sday = specdays[0][:-2]
        p = None
        prints('--------------', mdb_name)
        mdb_names = mdb_name.lower().split('_')
        prints('-----------', mdb_names, cfgpath)
        mdbtablename = tablename[:tablename.rfind('_')]
        shellcmd = "mdb_transfer %s%s%s_%s.%s %s_%s %s 40 export 20000  oracle " % (
        cfgpath, os.sep, mdb_names[0], mdbtablename, 'mdb', tablename, sday, strConn)
        prints('---------------', shellcmd)
        if not parrallel:
            # ret,msg = exportMdbTable(mdb_ip,mdb_port,tablename,strConn,cfgpath)
            result = runSSHcmd(hostip, user, pwd, shellcmd)
            if result[shellcmd][0] == False:
                return ret, result[shellcmd][1]
        else:
            p = multiprocessing.Process(target=runSSHcmd, args=(hostip, user, pwd, shellcmd))
            if p != None:
                procLst.append(p)
                p.start()
    if procLst != []:
        waiting4threads(3, procLst, caseId, dbObj)
    return True, ''
    # mdb_transfer_ex(mdb_ip, mdb_port, cfgpath+'/'+mdbcfgfile, strConn, tablename, suffix,caseinfo)
