
@@database/URSUS/URSUS.DDL_EVENT_TYPE.TYP

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


@@database/URSUS/URSUS.DDL_EVENT_TRIGGER.TRI

@@database/URSUS/URSUS.SCHEMA_PARAMS.TAB
@@database/URSUS/URSUS.MAPS.TAB
@@database/URSUS/URSUS.PROCESS_DDL_EVENTS.PAK
@@database/URSUS/URSUS.PROCESS_DDL_EVENTS.PLS
@@database/URSUS/URSUS.ADMIN_UI.PAK
@@database/URSUS/URSUS.ADMIN_UI.PLS


grant execute on PROCESS_DDL_EVENTS to URSUS_CONNECTOR;
grant execute on ADMIN_UI to URSUS_CONNECTOR;
-- Something strange going on with caller rights being in effect inside packages
grant select on schema_params to ursus_connector;
grant select on maps to ursus_connector;


begin DBMS_AQADM.grant_queue_privilege ( 
   privilege     =>     'ALL',
   queue_name    =>     'URSUS.AQ_DDL_EVENT_QUEUE', 
   grantee       =>     'URSUS_CONNECTOR', 
   grant_option  =>      FALSE);
end;
/
