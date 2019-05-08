CREATE OR REPLACE PACKAGE BODY CONTROL_UI as

  procedure git_commit(p_schema varchar2,p_message varchar) as
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_enqueue_options    dbms_aq.enqueue_options_t;
    l_message_properties dbms_aq.message_properties_t;
    l_message_handle     RAW(16);
    l_msg ddl_event_type;

  begin

    l_msg := ddl_event_type( 'GIT_COMMIT' ,p_schema , sys_context('USERENV','OS_USER'),
			ora_instance_num, ora_database_name,p_schema ,
      '-','-',p_message);

    DBMS_AQ.ENQUEUE(
      queue_name         => 'URSUS.AQ_DDL_EVENT_QUEUE',
      enqueue_options    => l_enqueue_options,
      message_properties => l_message_properties,
      payload            => l_msg,
      msgid              => l_message_handle
    );
    commit;
  end git_commit;

end control_ui;
/
