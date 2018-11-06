from getpass import getpass,getuser
import cx_Oracle 
import logging
import traceback
from collections import namedtuple
import re
import pprint


object_type_map = {
    'PACKAGE BODY':'PACKAGE_BODY',
    'PACKAGE':'PACKAGE_SPEC',
    'TYPE BODY':'TYPE_BODY',
    'TYPE':'TYPE_SPEC',
    'JOB':'PROCOBJ'
    }

def rows_as_dicts(cursor):
    """ returns cx_Oracle rows as dicts """
    colnames = [i[0].lower() for i in cursor.description]
    logging.debug("colnames:"+str(colnames))
    for row in cursor:
        yield dict(zip(colnames, row)) 

class DDLEvent(object):
    def __init__(self, d):
        self.__dict__ = d

class SchemaParams(object):
    def __init__(self, d):
        self.__dict__ = d

class DDLHandler:
    def __init__(self,config):
        self.db_connect_string = config.get('DATABASE','ConnectString')
        self.db_username = config.get('DATABASE','Username')
        self.db_schema = config.get('DATABASE','Schema')
        
        pw=getpass("Oracle password for %s:" % (self.db_username) )

        self.con = cx_Oracle.connect(user=self.db_username,password=pw,dsn=self.db_connect_string)
        self.cur = self.con.cursor()

        # sysevent out VARCHAR2,login_user out VARCHAR2,instance_num out NUMBER,database_name out VARCHAR2, 
        #    obj_owner out VARCHAR, obj_name out VARCHAR, obj_type out VARCHAR, sql_text out CLOB
        self.sysevent = self.cur.var(cx_Oracle.STRING)
        self.login_user = self.cur.var(cx_Oracle.STRING)
        self.os_user = self.cur.var(cx_Oracle.STRING)
        self.instance_num = self.cur.var(cx_Oracle.NUMBER)
        self.database_name = self.cur.var(cx_Oracle.STRING)
        self.obj_owner = self.cur.var(cx_Oracle.STRING)
        self.obj_name = self.cur.var(cx_Oracle.STRING)
        self.obj_type = self.cur.var(cx_Oracle.STRING)
        self.sql_text = self.cur.var(cx_Oracle.CLOB)
        ##self.rc_schema_params = self.cur.var(cx_Oracle.CURSOR)
        self.cur.prepare("""
            begin 
                %s.process_ddl_events.RECV(:sysevent,:login_user,:os_user,:instance_num,:database_name,
                    :obj_owner,:obj_name,:obj_type,:sql_text,:rc_schema_params,:wait_time);
            end;
        """% (self.db_schema ))

        self.src_cur = self.con.cursor()

        self.src_cur.prepare("""

                    begin :res := %s.process_ddl_events.get_ddl(:object_owner,:object_name,:object_type); end;

                    """ % (self.db_schema ))

        self.map_cur = self.con.cursor()

        self.map_result = self.map_cur.var(cx_Oracle.STRING)
        self.map_cur.prepare("""

                    begin :res := %s.process_ddl_events.map(:map_name,:key,:default_value); end;

                    """ % (self.db_schema ))

    def recv_next(self):
        try:
            rc_schema_params = self.cur.var(cx_Oracle.CURSOR)
            self.cur.execute(None,(self.sysevent, self.login_user, self.os_user, self.instance_num,self.database_name,
                self.obj_owner,self.obj_name,self.obj_type,self.sql_text,rc_schema_params,10))
        except cx_Oracle.DatabaseError as ex:
            error, = ex.args
            if(error.code == 25228):
                logging.debug("Receive timeout!")
                return
            else:
                raise

        logging.info("sysevent:%s, login_user:%s, os_user:%s owner:%s, name:%s, type:%s" %(self.sysevent.getvalue(),self.login_user.getvalue(),
            self.os_user.getvalue(),self.obj_owner.getvalue(), self.obj_name.getvalue(),self.obj_type.getvalue()))
        try:
            logging.info("Statement:%s"+(self.sql_text.getvalue()))
        except:
            logging.info('No SQL statement Collected')
            logging.debug(traceback.format_exc())
        curs_schema_params =  rc_schema_params.getvalue()
        rs_schema_params = []
        for row in rows_as_dicts(curs_schema_params):
            #print(row)
            rs_schema_params.append(SchemaParams(row))
        logging.debug("Schema Params1:"+pprint.pformat(rs_schema_params))
        try:
            schema_params = rs_schema_params[0]
        except IndexError:
            schema_params=None
            
        logging.debug("Schema Params:"+pprint.pformat(schema_params))

        return DDLEvent({"sysevent":self.sysevent.getvalue(),"login_user":self.login_user.getvalue(),
            "os_user":self.os_user.getvalue(),"obj_owner":self.obj_owner.getvalue(),"obj_name": self.obj_name.getvalue(),
            "obj_type":self.obj_type.getvalue(), "schema_params":schema_params
        })

    def commit(self):
        self.con.commit()

    def get_source(self,schema,object_name,object_type):
        logging.debug ("Getting source for %s.%s : %s"%(object_type,schema,object_name))
        src = self.src_cur.var(cx_Oracle.CLOB)
        self.src_cur.execute(None,{'res':src,'object_type':object_type_map.get(object_type , object_type),'object_name':object_name,'object_owner':schema }) ##,'schema':schema})
        return src

    def map(self,map_name,key,default_value):
        logging.debug ("Mapping  %s -> %s "%(map_name,key))
        self.map_cur.execute(None,{'res':self.map_result,'map_name':map_name,'key':key,'default_value':default_value }) ##,'schema':schema})
        return self.map_result.getvalue()

    def get_source_lines(self,schema,object_name,object_type):
        src = self.get_source(schema,object_name,object_type,)
        lines = (src.getvalue().read()+'\n').splitlines(1)
        if lines[0] == '\n':
            lines.pop(0)
        lines[0]=re.sub('^ +','',lines[0])
        lines[0]=re.sub(' +$','',lines[0])
        lines[0]=re.sub('\"','',lines[0])
        lines[0]=re.sub(schema+'\.','',lines[0])
        lines[0]=re.sub('EDITIONABLE\s','',lines[0])
        lastline = lines.pop()
        while re.search('^(/|--)',lastline ):
            lastline = lines.pop()
        if not re.search('^\s*$',lastline):
            lines.append(lastline)
        lines.append("/")
        lines = [ re.sub(' +$','',line)  for line in lines ]
        return lines
        #head = lines[:-10]
        #tail = [line for line in lines[-10:] if re.search('^--', line ) == None]
        #return head+tail
        #return [line for line in lines if re.search('^--', line ) == None]

    def list_schema_objects(self,schema):
        cur = self.con.cursor()
        rc = cur.var(cx_Oracle.CURSOR)
        rc_schema_params = cur.var(cx_Oracle.CURSOR)
        cur.prepare("""
                begin 
                    :rc := %s.admin_ui.list_schema_objects(:p_owner,:rc_schema_params);
                end;    
            """ % ( self.db_schema))

        cur.execute(None,{'rc':rc,'p_owner':schema,'rc_schema_params':rc_schema_params})
        curs_schema_params =  rc_schema_params.getvalue()
        rs_schema_params = []
        for row in rows_as_dicts(curs_schema_params):
            #print(row)
            rs_schema_params.append(SchemaParams(row))
        logging.debug("Schema Params1:"+pprint.pformat(rs_schema_params))
        try:
            schema_params = rs_schema_params[0]
        except IndexError:
            schema_params=None


        for row in rows_as_dicts(rc.getvalue()):
            yield DDLEvent({"sysevent":'INIT',
            "os_user":getuser(),"obj_owner":row['owner'],"obj_name": row['object_name'] ,
            "obj_type":row['object_type'], "schema_params":schema_params
            })
