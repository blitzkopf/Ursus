create user ursus identified by &&password
  QUOTA UNLIMITED ON users;

grant connect to ursus;
grant resource to ursus;

grant create type to ursus;
grant create table to ursus;
grant create table to ursus;
grant create procedure to ursus;

GRANT aq_administrator_role TO ursus;
grant select_catalog_role to ursus;

grant select on dba_objects to ursus;
grant select on dba_views to ursus;

grant execute on dbms_aqadm to ursus;
grant execute on dbms_aq to ursus;

grant create any trigger to ursus;
grant administer database trigger to ursus;-- required for ON DATABASE
grant alter any trigger to ursus;
grant drop any trigger to ursus;