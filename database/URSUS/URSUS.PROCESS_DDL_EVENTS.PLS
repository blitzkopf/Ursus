CREATE OR REPLACE PACKAGE BODY PROCESS_DDL_EVENTS AS

  procedure recv( sysevent out VARCHAR2,login_user out VARCHAR2,os_user out VARCHAR2,instance_num out NUMBER,database_name out VARCHAR2,
    obj_owner out VARCHAR, obj_name out VARCHAR, obj_type out VARCHAR, obj_status out varchar2 , sql_text out CLOB,rc_schema_params out sys_refcursor ,  wait_time number default null)
  AS
    l_dequeue_options    DBMS_AQ.DEQUEUE_OPTIONS_T;
    l_message_properties DBMS_AQ.MESSAGE_PROPERTIES_T;
    l_message_handle     RAW(16);
    l_msg            		 ddl_event_type;
  begin

		if	wait_time is not null then
			l_dequeue_options.wait := wait_time;
		end if;
    DBMS_AQ.DEQUEUE(
      queue_name         => 'URSUS.aq_ddl_event_queue',
      dequeue_options    => l_dequeue_options,
      message_properties => l_message_properties,
      payload            => l_msg,
      msgid              => l_message_handle
      );

    sysevent := l_msg.sysevent;
    login_user := l_msg.login_user;
		os_user := l_msg.os_user;
    instance_num := l_msg.instance_num;
    database_name := l_msg.database_name;
    obj_owner := l_msg.obj_owner;
    obj_name := l_msg.obj_name;
    obj_type := l_msg.obj_type;
    sql_text := l_msg.sql_text;

    begin
      select status
        into obj_status
      from dba_objects
      where owner=obj_owner
        and object_name = obj_name
        and object_type = obj_type;
    exception when no_data_found then
      obj_status := NULL;
    end;

    open rc_schema_params for
      select *
      from ursus.schema_params
      where schema = l_msg.obj_owner;

  END recv;

  function get_ddl(p_obj_owner varchar2,p_obj_name varchar2, p_obj_type varchar2 ) return clob
  as
      dummy clob;
      prefix varchar2(10) := '   ';
      res CLOB;
      l_cursor integer ;
      l_n          NUMBER;
      l_long_val   VARCHAR2(4000);
      l_long_len   NUMBER;
      l_buflen     NUMBER := 4000;
      l_curpos     NUMBER := 0;
  begin
    if(p_obj_type = 'VIEW' ) then
        res := 'CREATE OR REPLACE FORCE VIEW '||p_obj_name||' ('||chr(10);
        for r in ( select column_name
          from SYS.dba_tab_cols
          where table_name = p_obj_name
              and owner = p_obj_owner
          order by column_id
        ) loop
          res := res || prefix||r.column_name ;
          prefix := ','||chr(10)||'   ';
        end loop;
        res := res || ' )'||chr(10)||'AS'||chr(10);
        l_cursor := dbms_sql.open_cursor;
        dbms_sql.parse( l_cursor,
        q'[select text
            from dba_views
            where view_name = :p_obj_name
                and owner=:p_obj_owner
            ]', dbms_sql.native );
        dbms_sql.bind_variable( l_cursor, 'p_obj_name', p_obj_name );
        dbms_sql.bind_variable( l_cursor, 'p_obj_owner', p_obj_owner );

        dbms_sql.define_column_long(l_cursor, 1);
        l_n := dbms_sql.execute(l_cursor);
        IF ( dbms_sql.fetch_rows(l_cursor) > 0 ) THEN
            LOOP
                dbms_sql.column_value_long(l_cursor,1,l_buflen,l_curpos,l_long_val,l_long_len);
                l_curpos := l_curpos + l_long_len;
                dummy := dummy ||l_long_val;
            EXIT WHEN l_long_len = 0;
        END LOOP;
        END IF;
        --return dummy;
        res := res || dummy || chr(10) || '/';
    else
      res := dbms_metadata.get_ddl(p_obj_type,p_obj_name,p_obj_owner);
    end if;

   return res;
  end;

  function map(p_map_name varchar2, p_key varchar2,p_default_value varchar2 default null) return varchar2
  as
    result varchar2(50);
  begin
    select value into result
    from ursus.maps
    where map_name = p_map_name
      and key = p_key;
    return result;
  exception
    when no_data_found then
       return p_default_value;
  end;

  procedure get_depend_priority(p_obj_owner varchar2, p_map1 varchar2, p_map2 varchar2, rc out sys_refcursor ) as
  begin
    open rc for
      q'[with depend  as (
        select referenced_owner owner,referenced_name name, referenced_type type,
            owner d_owner,name d_name,type d_type
        from dba_dependencies
        where (referenced_owner,referenced_name,referenced_type) not
          in (
            ('SYS',	'STANDARD',	'PACKAGE')
          )
        union all
        select table_owner owner,table_name name,'TABLE' type,
            owner d_owner, index_name d_name, 'INDEX' d_type
        from dba_indexes
        ),  rdep(owner,name,type, d_owner, d_name,  d_type, lvl ) as
        (
            select d.*, cast ( 1  as number ) lvl
            from depend d
            where owner=:p_obj_owner
            union all
            select d.owner,d.name,d.type,d.d_owner,d.d_name, d.d_type, r.lvl + 1 lvl
            from rdep r
              join depend d on d.owner=r.d_owner and d.name=r.d_name and d.type = r.d_type
        ) cycle owner,name,type set looped  to 'Y' default 'u'
        --select * from depend;
        select d_owner,d_name,d_type,max_level, m1.value type_mapped1, m2.value type_mapped2
        from (
            select d_owner,d_name,d_type , max(lvl) max_level
            from rdep
            group by d_owner,d_name,d_type
            order by max_level desc
            ) s
            left join ursus.maps m1 on m1.map_name = :p_map1 and m1.key=s.d_type
            left join ursus.maps m2 on m2.map_name = :p_map2 and m2.key=s.d_type
        ]' using p_obj_owner,p_map1, p_map2;

  end get_depend_priority;

END PROCESS_DDL_EVENTS;
/
