#!/usr/bin/env pythonself.lock.acquire()
# coding=gbk

import sys, os

try:
    import cx_Oracle
except:
    pass

try:
    import MySQLdb
except Exception as e:
    pass
import multiprocessing
from threading import Lock

from time import sleep

DBOPMAP2 = {}
DBOPMAP = {}


def getDBConn(strConn, stringType=0):
    """Get the database from database connect string.
       --input parameter
       strConn: database connect string .
       The oracle connect string like this:
       oracle:username/passwd@cu_test_10gr2
       oracle:username,passwd,127.0.0.1:1521:ora120h
       The mysql connect string like this:
       mysql:-uadmin -padmin -h10.10.10.10 -P3306 -Datpdev
       mysql:host='10.10.12.171',user='admin',passwd='admin',db='atpdev',port=3306,charset='utf8',connect_timeout=10       
       The header define the database type ,the default database type is oracle.
       --output parameter
       1:Oracle 
       2:MySQL          
       create by limf2@asiainfo.com
    """
    pass
    strConn = strConn.replace('"', '')
    sList = strConn.split(":")
    if len(sList) > 1:
        dbString = sList[0]
        if dbString.upper() == 'MYSQL':
            mysqlString = ':'.join(sList[1:])
            newMysqlParaList = []
            # printstringType
            if mysqlString.strip().startswith('-') and stringType == 2:
                mysqlList = mysqlString.split("-")
                for mysqlPara in mysqlList:
                    x = mysqlPara.strip()[:1]
                    xx = {'u': 'user = ',
                          'p': 'passwd = ',
                          'h': 'host = ',
                          'P': 'port = ',
                          'D': 'db = ',
                          'C': 'compress = '
                          }
                    try:
                        if xx[x] == xx['P']:
                            newMysqlParaList.append(xx[x] + mysqlPara[1:])
                        else:
                            newMysqlParaList.append(xx[x] + '\'' + mysqlPara[1:].replace(' ', '') + '\'')
                    except:
                        pass
                    mysqlString = ','.join(newMysqlParaList)
            elif stringType == 1:
                mysqlList = mysqlString.split(",")
                for mysqlPara in mysqlList:
                    x = mysqlPara.strip().split('=')[0].strip()
                    xx = {'user': '-u',
                          'passwd': '-p',
                          'host': '-h',
                          'port': '-P',
                          'db': '-D',
                          'compress': '-C'
                          }
                    try:
                        newMysqlParaList.append(xx[x] + mysqlPara.strip().split('=')[1])
                    except:
                        pass
                    mysqlString = ' '.join(newMysqlParaList)
            return 2, mysqlString

        else:
            if dbString.upper().startswith('ORACLE'):
                return 1, ':'.join(sList[1:])
            else:
                return 1, ':'.join(sList[:])
    else:
        return 1, strConn


def assertDbtablenull(conn, tablist, intResult):
    cur = conn.cursor()
    flag = 1

    for item in tablist:
        if len(item) <= 0:
            continue

        try:
            cur.execute('select count(*) from %s ' % item)
            flag = 1
        except cx_Oracle.DatabaseError as e:
            flag = 0

        if flag == 1:
            each_rec = cur.fetchall()
            if len(each_rec) > 0:
                if intResult == 0 and each_rec[0][0] == 0 or intResult != 0 and each_rec[0][0] != 0:
                    continue
                else:
                    return False, each_rec[0][0]
        else:
            return False, -1

    cur.close()
    return True, 0


def cleanTables(strConn, listTables):
    try:
        con = cx_Oracle.connect(strConn)
    except cx_Oracle.DatabaseError as e:
        return -1, e

    failList = []
    cur = con.cursor()
    for tablename in listTables:
        try:
            cur.execute('truncate table %s' % tablename)
        except cx_Oracle.DatabaseError as e:
            failList.append(tablename)
            continue

    for tablename in failList:
        try:
            cur.execute('delete from %s' % tablename)
        except cx_Oracle.DatabaseError as e:
            continue

    con.commit()
    con.close()

    return 0, 'truncate tables: %s' % ','.join(listTables)


def droptables(strConn, listTabs):
    DBTYPE, CONNSTR = getDBConn(strConn, 2)
   
    if DBTYPE == 2:
        conCmd = 'con = MySQLdb.connect(' + CONNSTR + ')'
        try:
            exec (conCmd)
        except MySQLdb.DatabaseError as e:
            return -1, e
    else:
        conCmd = 'con = cx_Oracle.connect(CONNSTR)'
        try:
            exec (conCmd)
        except cx_Oracle.DatabaseError as e:
            return -1, e

    failList = []
    cur = con.cursor()
    for tablename in listTabs:
        try:
            cur.execute('drop table %s' % tablename)
        except:
            continue

    con.commit()
    con.close()

    return 0, 'drop tables: %s' % ','.join(listTabs)


def doOracleSQL(strConn, sqldirorfile, interval, tmpath):
    import shutil
    filelist = []
    if os.path.isdir(sqldirorfile):
        rellist = os.listdir(sqldirorfile)
        for item in rellist:
            filename = os.path.join(sqldirorfile, item)
            if os.path.isfile(filename):
                filelist.append(filename)
    else:
        if os.path.isfile(sqldirorfile):
            filelist.append(sqldirorfile)

    ret_code = True
    msg = ''
    if len(filelist) > 0:
        for item in filelist:
            sqlfile = os.path.join(tmpath, os.path.basename(item) + '.sql')
            try:
                shutil.copy(item, sqlfile)
            except OSError as e:
                ret_code = False
                msg = str(e)
                break
            try:
                ff = open(sqlfile, 'a')
                ff.write('\nexit;\n')
                ff.close()
            except OSError as e:
                pass

            strcmd = 'sqlplus -s %s @%s' % (strConn, sqlfile)
            outmsg, errmsg, result = runShell(strcmd, 600)
            if result == 0:
                pass
            else:
                ret_code = False
                msg = errmsg
                break

            if interval > 0:
                sleep(interval)

    return ret_code, msg


def doDBSQL(strConn, sqldirorfile, interval, tmpath):
    import shutil
    dbType, strConn = getDBConn(strConn)
    filelist = []
    if os.path.isdir(sqldirorfile):
        rellist = os.listdir(sqldirorfile)
        for item in rellist:
            filename = os.path.join(sqldirorfile, item)
            if os.path.isfile(filename):
                filelist.append(filename)
    else:
        if os.path.isfile(sqldirorfile):
            filelist.append(sqldirorfile)

    ret_code = True
    msg = ''
    if len(filelist) > 0:
        for item in filelist:
            sqlfile = os.path.join(tmpath, os.path.basename(item) + '.sql')
            try:
                shutil.copy(item, sqlfile)
            except OSError as e:
                ret_code = False
                msg = str(e)
                break
            try:
                ff = open(sqlfile, 'a')
                if dbType == 2:
                    pass
                else:
                    ff.write('\nexit;\n')
                ff.close()
            except OSError as e:
                pass

            if dbType == 2:
                strcmd = 'mysql %s <%s' % (strConn, sqlfile)
            else:
                strcmd = 'sqlplus -s %s @%s' % (strConn, sqlfile)
            
            outmsg, errmsg, result = runShell(strcmd, 600)
            if result == 0:
                pass
            else:
                ret_code = False
                msg = errmsg
                break

            if interval > 0:
                sleep(interval)

    return ret_code, msg


class DBObj(object):
    IGNORED_COMMAND = {'drop table': ['ORA-00942'], 'truncate table': ['ORA-00942'], 'rename': ['ORA-04043']
                       # ,'create table':['ORA-00955']
                       }

    def __init__(self, connStr):
        global DBOPMAP, DBOPMAP2
        self.dbopMap = DBOPMAP
        self.lock = Lock()
        self.lock2 = multiprocessing.Lock()
        self.syslog = None
        self.connstr = connStr
        print('-----connstr:----', connStr)
        self.DBTYPE, self.CONNSTR = getDBConn(connStr)
        if self.DBTYPE == 2:
            self.dbopMap = DBOPMAP2
        elif self.DBTYPE == 1:
            self.dbopMap = DBOPMAP
        self.con = None
        self.connect(self.CONNSTR, self.DBTYPE)
        self.testflag = False
        self.count = 0
        self.failcount = 0
        self.updateflag = False

    def commit(self):
        if self.updateflag == True:
            self.con.commit()
            self.updateflag = False

    def rollback(self):
        if self.updateflag == True:
            self.con.rollback()
            self.updateflag = False

    def settestflag(self, flag):
        self.testflag = flag

    def __del__(self):
        if self.con != None:
            try:
                self.con.close()
            except Exception as e:
                print('atdb:__del__:' + str(e))
        self.con = None

    def getDBMAP(self, dbconn=''):
        global DBOPMAP, DBOPMAP2
        if self.dbopMap != {}:
            return self.dbopMap
        if self.DBTYPE == 1 and DBOPMAP != {}:
            self.dbopMap = DBOPMAP
            return self.dbopMap
        if self.DBTYPE == 2 and DBOPMAP2 != {}:
            self.dbopMap = DBOPMAP2
            return self.dbopMap
        if dbconn != '':
            dbobj = DBObj(dbconn)
        else:
            dbobj = self

        sql = '''select name,sql from at_tc_sql_config where dbtype = '%s' ''' % (str(self.DBTYPE))
        lst, msg = dbobj.execute(sql, True)
        if dbobj.executefailed(msg):
            if dbobj != self:
                dbobj.__del__()
            return {}
        for item in lst:
            name = item[0]
            sql = item[1]
            self.dbopMap[name] = sql
            if dbobj != self:
                dbobj.__del__()
        print(self.dbopMap)
        if self.DBTYPE == 1:
            DBOPMAP = self.dbopMap
        elif self.DBTYPE == 2:
            DBOPMAP2 = self.dbopMap
        return self.dbopMap

    def setsyslog(self, syslog):
        self.syslog = syslog

    def executefailed(self, msg):
        if msg != '' and msg.find(' finished') == -1:
            return True
        return False

    def connect(self, CONNSTR, DBTYPE, con='self.con'):
        if DBTYPE == 2:
            conCmd = con + " = MySQLdb.connect(" + CONNSTR + ")"
        else:
            conCmd = con + " = cx_Oracle.connect('" + CONNSTR + "')"
        try:
            if self.syslog != None:
                self.syslog.info('conCmd = %s' % (conCmd))
            exec (conCmd)
            print(con, self.con)
            return True, con
        except Exception as e:
            if self.syslog != None:
                self.syslog.error('---%s' % (str(e)))
            print('err:%s' % e)
        return False, None

    def getsql(self, sqlName, paras=None):
        self.getDBMAP()

        if sqlName in self.dbopMap.keys():
            sql = self.dbopMap[sqlName]
            if self.syslog != None:
                self.syslog.debug('getsql:sql=%s,paras = %s' % (sql, str(paras)))
            if paras is not None:
                sql = self.replace(sql, paras)
                if self.syslog != None:
                    self.syslog.debug('getsql replaced:%s' % (sql))
            sql = sql.replace('\r\n', '')
            sql = sql.replace('\n', '')
            sql = sql.replace('\r', '')

            return sql
        return ''

    def replaceWithFlag(self, sql, paras, flags):
        v = []
        v1 = [sql]
        alllst = []
        if str(type(paras)).find('tuple') < 0:
            paras = [paras]
        for item in flags:
            alllst = []
            for it1 in v1:
                v = it1.split(item)
                for it2 in v:
                    alllst.append(it2)
            v1 = alllst
        sql1 = ''
        i = 0
        if self.syslog != None:
            self.syslog.debug('alllst:%s,sql:%s,paras:%s' % (alllst, sql, paras))
        if len(paras) < len(alllst) - 1:
            return ''
        for item in alllst:
            if i < len(paras):
                sql1 += item + str(paras[i])
            else:
                sql1 += item
            i += 1
        return sql1

    def replace(self, sql, paras):
        flags = ['%s', '%d']
        return self.replaceWithFlag(sql, paras, flags)

    def NeedReconnect(self, msg):
        badcode = ['ORA-01013', 'ORA-00026', 'ORA-00028', 'ORA-00030', 'ORA-00029', 'ORA-24909']
        reconn = False
        for it in badcode:
            if msg.find(it) > -1:
                reconn = True
                break
        return reconn

    def reconnect(self):
        try:
            self.lock2.acquire()
            self.lock.acquire()
            self.con.rollback()
            self.updateflag = False
            self.con.close()

        except Exception as e:
            if self.syslog != None:
                self.syslog.error('atdb.execute:' + str(e))
            self.lock.release()
            self.lock2.release()
            return False, str(e)
        ret, con = self.connect(self.CONNSTR, self.DBTYPE)
        if ret == False:
            self.lock.release()
            self.lock2.release()
            return False, 'connet dbobj failed'
        self.lock.release()
        self.lock2.release()
        return True, ''

    def execute(self, sql, isClob=False, commitImmi=True):
        print(sql)
        if sql.strip() == '':
            print(' sql is empty ')
            return [], 'sql is empty'
        ret, msg = self.execute2(sql, isClob)
        if msg.find('finished') == -1:
            reconn = self.NeedReconnect(msg)
            print('execute:needreconnect:', reconn, msg, sql)
            if not reconn:
                return ret, msg
            ret, msg = self.reconnect()
            if not ret:
                print('execute:reconnect:', ret, msg, sql)
                return ret, msg
            ret, msg = self.execute2(sql, isClob)
        return ret, msg

    def execute2(self, sql, isClob=False, commitImmi=True):
        if sql == '':
            return [], 'Empty SQL statement.'
        sql = sql.rstrip(';\n')
        sql = sql.rstrip(';')
        if self.syslog != None:
            self.syslog.info('execute: %s' % sql)
        firstword = sql.lstrip().split(' ')[0]
        cur = None
        if firstword == 'select':
            resList = []
            try:
                self.lock2.acquire()
                self.lock.acquire()
                self.count += 1
                cur = self.con.cursor()
                ret = cur.execute(sql)
                if isClob:
                    while True:
                        onerecord = cur.fetchone()
                        if onerecord == None:
                            break
                        newonerecord = []
                        for cit in onerecord:
                            if str(type(cit)).find('LOB') == -1:
                                newonerecord.append(cit)
                            else:
                                newonerecord.append(cit.read())
                        resList.append(newonerecord)

                else:
                    resList = cur.fetchall()
                if self.syslog != None:
                    self.syslog.debug(str(ret))
                cur.close()
            except Exception as e:
                self.failcount += 1
                if self.syslog != None:
                    self.syslog.error(str(e) + 'sql:' + sql)
                    # self.con.close()
                if cur != None:
                    cur.close()
                self.lock.release()
                self.lock2.release()
                return [], str(e)
            self.lock.release()
            self.lock2.release()
            return resList, firstword + ' finished'
        elif firstword in ('insert', 'delete', 'update', 'drop', 'create', 'commit', 'rename', 'truncate'):

            try:
                self.lock2.acquire()
                self.lock.acquire()
                self.count += 1
                cur = self.con.cursor()
                if self.testflag == False:
                    ret = cur.execute(sql)
                    if self.syslog != None:
                        self.syslog.debug(str(ret))
                    if self.updateflag == False:
                        self.updateflag = True
                    if commitImmi == True:
                        self.con.commit()
                        self.updateflag = False
                    cur.close()
            except Exception as e:
                print(str(e))
                self.failcount += 1
                if cur != None:
                    cur.close()
                self.lock.release()

                if self.syslog != None:
                    self.syslog.error(str(e))
                self.lock2.release()
                return [], str(e)
            self.lock.release()
            self.lock2.release()
            return [], firstword + ' finished'
        else:
            try:
                self.lock2.acquire()
                self.lock.acquire()
                self.count += 1
                cur = self.con.cursor()
                ret = cur.execute(sql)
                if self.syslog != None:
                    self.syslog.debug(str(ret))
                if self.updateflag == False:
                    self.updateflag = True
                if commitImmi == True:
                    self.con.commit()
                    self.updateflag = False
                cur.close()
                return [], firstword + ' finished'
            except Exception as e:
                print(str(e))
                self.failcount += 1
                if cur != None:
                    cur.close()
                if self.syslog != None:
                    self.syslog.error(str(e))
                return [], str(e)
            finally:
                self.lock.release()
                self.lock2.release()
        return [], firstword + ' failed'

    def execute4clob(self, sql, paravals, con=None, commitImmi=True):
        sql = sql.rstrip(';')
        ret, msg = self.execute4clob2(sql, paravals, con, commitImmi)
        if self.executefailed(msg):
            print(self.__class__, 'execute4clob first:', ret, msg)
            # reconn = self.NeedReconnect(msg)
            # print('execute4clob:needReconnect:',reconn,msg)
            # if reconn:
            #    ret,msg  = self.reconnect()
            #    if not ret:
            #        print('execute4clob:reconnect:',reconn,msg)
            #        return ret,msg
            #    ret,msg = self.execute4clob2(sql,paravals)
            #    return ret,msg
            ret, msg = self.execute4clob2(sql, paravals, con, commitImmi)
            print(self.__class__, 'execute4clob twice:', ret, msg)
        return ret, msg

    def execute4clob2(self, sql, paravals, con=None, commitImmi=True):
        paras = len(paravals)
        p = []
        cur = None
        self.lock2.acquire()
        self.lock.acquire()
        if con == None:
            ret, con = self.connect(self.connstr, self.DBTYPE, 'con')

        if con == None:
            self.lock.release()
            self.lock2.release()
            return [], 'connect failed:' + self.connstr + str(self.DBTYPE)
        try:

            self.count += 1
            cur = con.cursor()

            for i in range(0, paras):
                p1 = cur.var(cx_Oracle.CLOB)
                p1.setvalue(0, str(paravals[i]))
                p.append(p1)
            print(p)
            ret = cur.execute(sql, p)

            if self.updateflag == False:
                self.updateflag = True
            if commitImmi == True:
                con.commit()
                self.updateflag = False
            cur.close()
            # con.close()
            print('con closed', con, self.con, con == self.con)
            self.lock.release()
            self.lock2.release()
            return [], 'execute finished '
        except Exception as e:
            self.failcount += 1
            if cur != None:
                cur.close()
            # con.close()
            print(str(e))
            self.lock.release()
            self.lock2.release()
            return [], str(e)

    def generatenullval(self, key, defaultvalue):
        if self.DBTYPE == 1:
            if defaultvalue == '':
                return ''.join(('decode(', key, ",null,'',", key, ')'))
            return ''.join(('decode(', key, ",null,'", defaultvalue, "',", key, ')'))
        if self.DBTYPE == 2:
            if defaultvalue == '':
                return ''.join(('(case ', key, " when null then ''  when '' then '' else '", key, ' end )'))
            return ''.join((
                           '(case ', key, " when null then '", defaultvalue, "' when '' then '", defaultvalue, "'else ",
                           key, ' end )'))
        return key

    def getdblnk(self, dbconn):
        dbuser = dbconn.split('/')[0]
        dbpwd = dbconn.split('/')[-1].split('@')[0]
        host = dbconn.split('@')[-1]
        sql = "select db_link from dba_db_links where lower(username)='%s' and lower(host) = '%s'" % (
        dbuser.lower().strip(), host.lower().strip())
        lst, msg = self.execute(sql)
        if len(lst) == 1:
            return lst[0][0]
        return ''

    def gettocharstr(self, val):
        if self.DBTYPE == 1:
            return '''to_char( ''' + val + ''' )'''
        else:
            return '''concat(''' + val.replace("'", "'''") + ''','') '''

    def gettocharloc(self, val):
        if self.DBTYPE == 1:
            return val.find('''to_char('''), len('to_char(')
        else:
            return val.find('''concat('''), len('concat(')

    def getconcatstr(self, *valargs):
        strconcat = ''
        if self.DBTYPE == 1:
            for val in valargs:
                strconcat += '||' + val
            strconcat = strconcat[2:]
        else:
            strconcat = 'concat('
            for val in valargs:
                val = val.replace("'", "'''")
                if val.strip() == '':
                    strconcat += "'" + val + "',"
                else:
                    strconcat += val + ','
            strconcat += "'')"
        return strconcat

    def getconcatstr4insert(self, insertsqls, *valargs):
        strconcat = ''
        if self.DBTYPE == 1:
            for val in valargs:
                strconcat += """'''||""" + val + """||''',"""
            strconcat = strconcat[:-1]
            strconcat = insertsqls + strconcat
            strconcat += ")'"
        else:
            strconcat = "concat(" + insertsqls + "','''',"
            for val in valargs:
                val = val.replace("'", "'''")
                if val != valargs[-1]:
                    strconcat += val + ",''',''',"
                else:
                    strconcat += val + ",'''',')'"
            strconcat += ')'
        return strconcat

    def gettableexistssql(self, tabname, dbname=''):
        sql = ''
        if dbname == '':
            dbname = self.connstr.split('/')[0]
        if self.DBTYPE == 1:
            sql = '''select count(1) from all_tables where lower(table_name) = '%s' and lower(owner) = '%s';''' % (
            tabname, dbname)
        else:
            sql = '''select count(1) from INFORMATION_SCHEMA.TABLES where lower(table_name)= '%s' and lower(TABLE_SCHEMA) = '%s' ''' % (
            tabname, dbname)
        return sql

    def getalltabcolumnsql(self, tabname, dbname=''):
        sql = ''
        if dbname == '':
            dbname = self.connstr.split('/')[0]
        if self.DBTYPE == 1:
            sql = '''select column_name,data_type from all_tab_columns a where lower(a.table_name) = lower('%s') and lower(a.owner) = lower('%s')''' % (
            tabname.strip(), dbname.strip())
        else:
            sql = '''select COLUMN_NAME,data_type from INFORMATION_SCHEMA.columns a where lower(a.table_name) = lower('%s') and lower(a.owner)  = lower('%s')''' % (
            tabname.strip(), dbname.strip())

        return sql

    def getwherestr(self, wherecond):
        if wherecond.strip() == '':
            return ''
        return ' where ' + wherecond

    def getgroupstr(self, groupcond):
        if groupcond.strip() == '':
            return ''
        return ' group by ' + groupcond

    def getoritabname(self, tabnameinfo):
        t1 = tabnameinfo.split('.')
        ti = ''
        if len(t1) > 1:
            ti = t1[1]
        else:
            ti = t1[0]
        t1 = ti.split(' ')
        ti = t1[0]
        return ti

    def getoricolname(self, colname):
        if colname == '':
            return ''
        lst = colname.split('.')
        return lst[-1].split(' ')[0]

    def isselectall(self, sql):
        ball = False
        idx1 = sql.find('select ')
        idx2 = sql.find(' * ')
        idx3 = sql.find(' .* ')
        if idx2 == -1:
            idx2 = idx3
        if idx1 == -1:
            return ball
        idx4 = sql[idx1 + 1:].find('select ')
        if idx4 < idx2 and idx4 > -1:
            return ball
        return True

    def getdefaultval(self, key, fea_chars_array):
        if self.isspecialkey(key, fea_chars_array) == True:
            return ""
        else:
            return "'0'"

    @staticmethod
    def shouldignored(line, msg, ignoredcmd):
        for key in ignoredcmd.keys():
            if line.find(key) == 0:  # startwithkey
                for item in ignoredcmd[key]:
                    if msg.find(item) >= 0:
                        return True
        return False
