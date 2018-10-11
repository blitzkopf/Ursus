--------------------------------------------------------
--  DDL for Type DDL_EVENT_TYPE
--------------------------------------------------------
CREATE OR REPLACE  TYPE "DDL_EVENT_TYPE" AS OBJECT(    
  sysevent VARCHAR2(20),
  login_user VARCHAR2(30),
	os_user varchar2(100),
  instance_num NUMBER,
  database_name VARCHAR2(50),
  obj_owner VARCHAR(30) ,
  obj_name VARCHAR(30),
  obj_type  VARCHAR(20),
  sql_text CLOB
  --ora_is_alter_column,
  --ora_is_drop_column (for ALTER TABLE events)
);

/


  BEGIN DBMS_AQADM.DROP_QUEUE_TABLE( Queue_table        => '"URSUS"."AQ_DDL_EVENT_TABLE"',
	force => True);
	end;
/
	


--------------------------------------------------------
--  DDL for Queue Table AQ_TEST_TABLE
--------------------------------------------------------

   BEGIN DBMS_AQADM.CREATE_QUEUE_TABLE(
     Queue_table        => '"URSUS"."AQ_DDL_EVENT_TABLE"',
     Queue_payload_type => 'URSUS.DDL_EVENT_TYPE',
     Sort_list          => 'ENQ_TIME');
  END;
/
--------------------------------------------------------
--  DDL for Queue AQ_TEST_QUEUE
--------------------------------------------------------

   BEGIN DBMS_AQADM.CREATE_QUEUE(
     Queue_name          => 'URSUS.AQ_DDL_EVENT_QUEUE',
     Queue_table         => 'URSUS.AQ_DDL_EVENT_TABLE',
     Queue_type          =>  0,
     Max_retries         =>  5,
     Retry_delay         =>  0,
     dependency_tracking =>  FALSE);
  END;
/

	BEGIN DBMS_AQADM.START_QUEUE(		
     Queue_name          => 'URSUS.AQ_DDL_EVENT_QUEUE');
end;
/

C--------------------------------------------------------
--  DDL for Trigger DDL_EVENT_TRIGGER
--------------------------------------------------------

CREATE OR REPLACE  TRIGGER "URSUS"."DDL_EVENT_TRIGGER" after
DDL on database
declare 
		l_enqueue_options    dbms_aq.enqueue_options_t;
    l_message_properties dbms_aq.message_properties_t;
    l_message_handle     RAW(16);
    l_msg ddl_event_type;
    l_sql_txt ora_name_list_t;
    n number;
    l_sql_txt_clob CLOB;
  begin
    n := ora_sql_txt(l_sql_txt);
    if(n>0) then
      l_sql_txt_clob := l_sql_txt(1);
      for i in 2..n loop
        dbms_lob.append(l_sql_txt_clob,l_sql_txt(i));
      end loop;
    ELSE
      l_sql_txt_clob := null;
    end if;

    l_msg := ddl_event_type( ora_sysevent ,ora_login_user , sys_context('USERENV','OS_USER'),
			ora_instance_num,ora_database_name,ora_dict_obj_owner ,
      ora_dict_obj_name,ora_dict_obj_type, null);

    DBMS_AQ.ENQUEUE(
      queue_name         => 'URSUS.AQ_DDL_EVENT_QUEUE',
      enqueue_options    => l_enqueue_options,
      message_properties => l_message_properties,
      payload            => l_msg,
      msgid              => l_message_handle
    );

   --COMMIT;

  end DDL_EVENT_TRIGGER;
/
ALTER TRIGGER "URSUS"."DDL_EVENT_TRIGGER" ENABLE;


@@database/URSUS/URSUS.SCHEMA_PARAMS.TAB
@@database/URSUS/URSUS.PROCESS_DDL_EVENTS.PAK
@@database/URSUS/URSUS.PROCESS_DDL_EVENTS.PLS

CREATE TABLE SCHEMA_PARAMS 
(
  SCHEMA VARCHAR2(32) NOT NULL 
, GIT_ORIGIN_REPO VARCHAR2(500) NOT NULL 
, SUBDIR VARCHAR2(500) NOT NULL 
, TYPE_PREFIX_MAP VARCHAR2(30) 
, TYPE_SUFFIX_MAP VARCHAR2(30) 
, FILENAME_TEMPLATE VARCHAR2(500) NOT NULL 
, CONSTRAINT SCHEMA_PARAMS_PK PRIMARY KEY 
  (
    SCHEMA 
  )
  ENABLE 
);
