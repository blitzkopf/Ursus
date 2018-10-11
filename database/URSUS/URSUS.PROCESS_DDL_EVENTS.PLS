CREATE OR REPLACE PACKAGE BODY PROCESS_DDL_EVENTS AS

  procedure recv( sysevent out VARCHAR2,login_user out VARCHAR2,os_user out VARCHAR2,instance_num out NUMBER,database_name out VARCHAR2,
    obj_owner out VARCHAR, obj_name out VARCHAR, obj_type out VARCHAR, sql_text out CLOB,rc_schema_params out sys_refcursor ,  wait_time number default null)
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
      queue_name         => 'aq_ddl_event_queue',
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

    open rc_schema_params for
      select *
      from schema_params
      where schema = l_msg.obj_owner;

  END recv;

  function get_ddl(p_obj_owner varchar2,p_obj_name varchar2, p_obj_type varchar2) return varchar
  as
      dummy varchar2(32000);
      prefix varchar2(10) := '   ';
      res varchar2(32000);
  begin
    if(p_obj_type = 'VIEW' ) then
      res := 'CREATE OR REPLACE VIEW '||p_obj_name||' ('||chr(10);
      for r in ( select column_name
          from SYS.all_tab_cols
          where table_name = p_obj_name
              and owner = p_obj_owner
          order by column_id
      ) loop
          res := res || prefix||r.column_name ;
          prefix := ','||chr(10)||'   ';
      end loop;
      res := res || ' )'||chr(10)||'AS'||chr(10);
      select text
          into dummy
      from all_views
      where view_name = p_obj_name
          and owner=p_obj_owner
      ;
      res := res || dummy;
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
    from maps
    where map_name = p_map_name
      and key = p_key;
    return result;
  exception
    when no_data_found then
       return p_default_value;
  end;


END PROCESS_DDL_EVENTS;
/