create user ursus identified by &&password
  QUOTA UNLIMITED ON users
  default tablespace users;

grant connect to ursus;
grant resource to ursus;

grant create type to ursus;
grant create table to ursus;
grant create table to ursus;
grant create procedure to ursus;

GRANT aq_administrator_role TO ursus;
grant select_catalog_role to ursus;

grant select on dba_objects to ursus with grant option ;
grant select on dba_views to ursus with grant option ;
grant select on dba_tab_cols to ursus;
grant select on dba_indexes to ursus with grant option;
grant select on dba_dependencies  to ursus with grant option;
grant select on dba_constraints to ursus with grant option;

grant execute on dbms_aqadm to ursus;
grant execute on dbms_aq to ursus;

grant create any trigger to ursus;
grant administer database trigger to ursus;-- required for ON DATABASE
grant alter any trigger to ursus;
grant drop any trigger to ursus;

create user ursus_connector identified by  "&&password2";

grant  connect to ursus_connector;
grant select on dba_objects to ursus_connector;
grant select on dba_views to ursus_connector  ;
grant select on dba_indexes to ursus_connector;
grant select on dba_dependencies  to ursus_connector;
grant select_catalog_role to ursus_connector;
