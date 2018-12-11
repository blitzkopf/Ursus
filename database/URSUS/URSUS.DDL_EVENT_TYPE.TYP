CREATE OR REPLACE TYPE DDL_EVENT_TYPE AS OBJECT(
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