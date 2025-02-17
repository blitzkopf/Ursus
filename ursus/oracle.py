"""Oracle database interface."""

import logging
import os
import pprint
import re
import traceback
from getpass import getpass, getuser

import oracledb

object_type_map = {
    "PACKAGE BODY": "PACKAGE_BODY",
    "PACKAGE": "PACKAGE_SPEC",
    "TYPE BODY": "TYPE_BODY",
    "TYPE": "TYPE_SPEC",
    "JOB": "PROCOBJ",
    "MATERIALIZED VIEW": "MATERIALIZED_VIEW",
    ## for some strange reason MVIEWs sometimes show up as SNAPSHOT , even if statement says create materialized view.
    "SNAPSHOT": "MATERIALIZED_VIEW",
}


def rows_as_dicts(cursor):
    """Returns oracledb rows as dicts."""
    colnames = [i[0].lower() for i in cursor.description]
    logging.debug("colnames:" + str(colnames))
    for row in cursor:
        yield dict(zip(colnames, row))


class _DDLEvent(object):
    def __init__(self, d):
        self.__dict__ = d

    def __str__(self):
        return str(self.__dict__)


class _SchemaParams(object):
    def __init__(self, d):
        self.__dict__ = d

    def __str__(self):
        return str(self.__dict__)


class DDLHandler:
    """Handler for DDL events.

    Connects to database and processes DDL events from AQ.
    """

    def __init__(self, config):
        """Initialize DDLHandler.

        Connects to database and prepares statements for efficient executions.
        """
        self.db_connect_string = config.get("DATABASE", "ConnectString")
        self.db_username = config.get("DATABASE", "Username")
        self.db_schema = config.get("DATABASE", "Schema")
        self.db_password = config.get("DATABASE", "Password")

        if not self.db_password:
            pw = getpass("Oracle password for %s:" % (self.db_username))
        else:
            pw = self.db_password
        os.environ["NLS_LANG"] = ".AL32UTF8"
        print("Connecting to %s as %s" % (self.db_connect_string, self.db_username))
        self.con = oracledb.connect(user=self.db_username, password=pw, dsn=self.db_connect_string)
        setup_cur = self.con.cursor()
        ## TODO: Make these parameter configurable.
        setup_cur.execute("""
            BEGIN
                DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SQLTERMINATOR', true);
                DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'SEGMENT_ATTRIBUTES', true);
                DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'STORAGE', true);
                DBMS_METADATA.set_transform_param (DBMS_METADATA.session_transform, 'TABLESPACE', false);
            END;
        """)
        setup_cur.close()

        self.cur = self.con.cursor()
        # sysevent out VARCHAR2,login_user out VARCHAR2,instance_num out NUMBER,database_name out VARCHAR2,
        #    obj_owner out VARCHAR, obj_name out VARCHAR, obj_type out VARCHAR, sql_text out CLOB
        self.sysevent = self.cur.var(oracledb.STRING)
        self.login_user = self.cur.var(oracledb.STRING)
        self.os_user = self.cur.var(oracledb.STRING)
        self.instance_num = self.cur.var(oracledb.NUMBER)
        self.database_name = self.cur.var(oracledb.STRING)
        self.obj_owner = self.cur.var(oracledb.STRING)
        self.obj_name = self.cur.var(oracledb.STRING)
        self.obj_type = self.cur.var(oracledb.STRING)
        self.sql_text = self.cur.var(oracledb.CLOB)
        self.obj_status = self.cur.var(oracledb.STRING)

        ##self.rc_schema_params = self.cur.var(oracledb.CURSOR)
        self.cur.prepare(
            """
            begin
                %s.process_ddl_events.RECV(:sysevent,:login_user,:os_user,:instance_num,:database_name,
                    :obj_owner,:obj_name,:obj_type,:obj_status,:sql_text,:rc_schema_params,:wait_time);
            end;
        """
            % (self.db_schema)
        )

        self.src_cur = self.con.cursor()

        self.src_cur.prepare(
            """

                    begin :res := %s.process_ddl_events.get_ddl(:object_owner,:object_name,:object_type); end;

                    """
            % (self.db_schema)
        )

        self.map_cur = self.con.cursor()

        self.map_result = self.map_cur.var(oracledb.STRING)
        self.map_cur.prepare(
            """

                    begin :res := %s.process_ddl_events.map(:map_name,:key,:default_value); end;

                    """
            % (self.db_schema)
        )
        self.priority_cur = self.con.cursor()
        self.priority_cur.prepare(
            """begin
                      %s.process_ddl_events.get_depend_priority(:object_owner,:map_name1,:map_name2,:rc_priority);
            end;
            """
            % (self.db_schema)
        )

        self.cons_ind_cur = self.con.cursor()

        self.cons_ind_cur.prepare(
            """
                    begin
                        :res := %s.process_ddl_events.is_constraint_index(:object_owner,:object_name,:object_type);
                    end;

                    """
            % (self.db_schema)
        )

    def recv_next(self):
        """Receive next event from AQ."""
        try:
            rc_schema_params = self.cur.var(oracledb.CURSOR)
            self.cur.execute(
                None,
                (
                    self.sysevent,
                    self.login_user,
                    self.os_user,
                    self.instance_num,
                    self.database_name,
                    self.obj_owner,
                    self.obj_name,
                    self.obj_type,
                    self.obj_status,
                    self.sql_text,
                    rc_schema_params,
                    10,
                ),
            )
        except oracledb.DatabaseError as ex:
            (error,) = ex.args
            if error.code == 25228:
                logging.debug("Receive timeout!")
                return
            else:
                raise

        logging.info(
            "sysevent:%s, login_user:%s, os_user:%s owner:%s, name:%s, type:%s"
            % (
                self.sysevent.getvalue(),
                self.login_user.getvalue(),
                self.os_user.getvalue(),
                self.obj_owner.getvalue(),
                self.obj_name.getvalue(),
                self.obj_type.getvalue(),
            )
        )
        try:
            sql_text = (
                self.sql_text.getvalue().read().rstrip("\0")
            )  ## Oracle adds a chr(0) to the CLOB for good measure.
            logging.info("Statement:%s" % (sql_text))
        except Exception as _ex:
            logging.info("No SQL statement Collected")
            logging.debug(traceback.format_exc())
        curs_schema_params = rc_schema_params.getvalue()
        rs_schema_params = []
        for row in rows_as_dicts(curs_schema_params):
            # print(row)
            rs_schema_params.append(_SchemaParams(row))
        logging.debug("Schema Params1:" + pprint.pformat(rs_schema_params))
        try:
            schema_params = rs_schema_params[0]
        except IndexError:
            schema_params = None

        logging.debug("Schema Params:" + pprint.pformat(schema_params))

        return _DDLEvent(
            {
                "sysevent": self.sysevent.getvalue(),
                "login_user": self.login_user.getvalue(),
                "os_user": self.os_user.getvalue(),
                "obj_owner": self.obj_owner.getvalue(),
                "obj_name": self.obj_name.getvalue(),
                "obj_type": self.obj_type.getvalue(),
                "obj_status": self.obj_status.getvalue(),
                "sql_text": sql_text,
                "schema_params": schema_params,
            }
        )

    def commit(self):
        """Commit changes to database."""
        self.con.commit()

    def get_source(self, schema, object_name, object_type):
        """Return source for object."""
        logging.debug("Getting source for %s.%s : %s" % (object_type, schema, object_name))
        src = self.src_cur.var(oracledb.CLOB)
        self.src_cur.execute(
            None,
            {
                "res": src,
                "object_type": object_type_map.get(object_type, object_type),
                "object_name": object_name,
                "object_owner": schema,
            },
        )
        return src

    def map(self, map_name, key, default_value):
        """Return mapped value."""
        logging.debug("Mapping  %s -> %s " % (map_name, key))
        self.map_cur.execute(
            None, {"res": self.map_result, "map_name": map_name, "key": key, "default_value": default_value}
        )
        return self.map_result.getvalue()

    def get_source_lines(self, schema, object_name, object_type):
        """Return source as list of lines.

        Tries to fix some common issues with source code.
        """
        src = self.get_source(
            schema,
            object_name,
            object_type,
        )
        lines = (src.getvalue().read() + "\n").splitlines(1)
        if lines[0] == "\n":
            lines.pop(0)
        lines[0] = re.sub("^ +", "", lines[0])
        lines[0] = re.sub(" +$", "", lines[0])
        lines[0] = re.sub(r"\"", "", lines[0])
        lines[0] = re.sub(schema + r"\.", "", lines[0])
        lines[0] = re.sub(r"EDITIONABLE\s", "", lines[0])
        lastline = lines.pop()
        while re.search("^(--)", lastline):
            lastline = lines.pop()
        if not re.search(r"^\s*$", lastline):
            lines.append(lastline)
        # if(object_type == 'TRIGGER' ) :
        #     # Triggers sometimes  missing the / between create and alter trigger statements
        #     lastline = lines.pop()
        #     keep = []
        #     print("backing")
        #     while re.search('^\s*ALTER',lastline ):
        #         keep.insert(0,lastline)
        #         lastline = lines.pop()
        #     lines.append(lastline)
        #     lines.append("/\n")
        #     lines.extend(keep)
        # lines.append("/")
        lines = [re.sub(" +$", "", line) for line in lines]
        return lines
        # head = lines[:-10]
        # tail = [line for line in lines[-10:] if re.search('^--', line ) == None]
        # return head+tail
        # return [line for line in lines if re.search('^--', line ) == None]

    def list_schema_objects(self, schema):
        """List objects in schema."""
        cur = self.con.cursor()
        rc = cur.var(oracledb.CURSOR)
        rc_schema_params = cur.var(oracledb.CURSOR)
        cur.prepare(
            """
                begin
                    :rc := %s.admin_ui.list_schema_objects(:p_owner,:rc_schema_params);
                end;
            """
            % (self.db_schema)
        )

        cur.execute(None, {"rc": rc, "p_owner": schema, "rc_schema_params": rc_schema_params})
        curs_schema_params = rc_schema_params.getvalue()
        rs_schema_params = []
        for row in rows_as_dicts(curs_schema_params):
            # print(row)
            rs_schema_params.append(_SchemaParams(row))
        logging.debug("Schema Params1:" + pprint.pformat(rs_schema_params))
        try:
            schema_params = rs_schema_params[0]
        except IndexError:
            schema_params = None

        for row in rows_as_dicts(rc.getvalue()):
            yield _DDLEvent(
                {
                    "sysevent": "INIT",
                    "os_user": getuser(),
                    "obj_owner": row["owner"],
                    "obj_name": row["object_name"],
                    "obj_type": row["object_type"],
                    "schema_params": schema_params,
                }
            )

    def get_schema_params(self, schema):
        """Return parameters for schema."""
        cur = self.con.cursor()
        cur.prepare(
            """
            begin
                %s.admin_ui.get_schema_params(:schema,:rc_schema_params);
            end;
        """
            % (self.db_schema)
        )
        rc_schema_params = cur.var(oracledb.CURSOR)
        cur.execute(None, (schema, rc_schema_params))
        # TODO There must be a more reasobale way to do this.
        rs_schema_params = []
        for row in rows_as_dicts(rc_schema_params.getvalue()):
            # print(row)
            rs_schema_params.append(_SchemaParams(row))
        logging.debug("Schema Params1:" + pprint.pformat(rs_schema_params))
        schema_params = rs_schema_params[0]
        return schema_params

    def set_schema_params(self, schema, params):
        """Set parameters for schema."""
        cur = self.con.cursor()
        cur.prepare(
            """
            begin
                %s.admin_ui.set_schema_params(p_schema => :schema,
                    p_git_origin_repo => :git_origin_repo,
                    p_subdir => :subdir,
                    p_type_prefix_map => :type_prefix_map,
                    p_type_suffix_map => :type_suffix_map,
                    p_filename_template => :filename_template,
                    p_build_system => :build_system);
            end;
        """
            % (self.db_schema)
        )
        cur.execute(
            None,
            (
                schema,
                params.git_origin_repo,
                params.subdir,
                params.type_prefix_map,
                params.type_suffix_map,
                params.filename_template,
                params.build_system,
            ),
        )
        self.con.commit()
        return self.get_schema_params(schema)

    def get_depend_priority(self, schema, map_name1, map_name2):
        """Return priority ordering from dependencies."""
        rc_priority = self.priority_cur.var(oracledb.CURSOR)
        self.priority_cur.execute(
            None, {"object_owner": schema, "map_name1": map_name1, "map_name2": map_name2, "rc_priority": rc_priority}
        )
        curs_priority = rc_priority.getvalue()
        rs_priority = []
        for row in rows_as_dicts(curs_priority):
            print(row)
            rs_priority.append(row)
        return rs_priority

    def get_delimiter(self, object_type):
        """Return delimiter for object type."""
        if object_type in ("PACKAGE BODY", "PACKAGE", "TYPE BODY", "TYPE", "FUNCTION", "PROCEDURE", "TRIGGER"):
            return "/"
        else:
            return ";"

    def is_constraint_index(self, schema, object_name, object_type):
        """Return 1 if object is an indexe used to enforce constraint, otherwise return 0."""
        logging.debug("Getting index info for %s.%s : %s" % (object_type, schema, object_name))
        is_constraint_index = self.cons_ind_cur.var(oracledb.NUMBER)
        self.cons_ind_cur.execute(
            None,
            {
                "res": is_constraint_index,
                "object_type": object_type_map.get(object_type, object_type),
                "object_name": object_name,
                "object_owner": schema,
            },
        )  ##,'schema':schema})
        return is_constraint_index
