CREATE OR REPLACE PACKAGE BODY ADMIN_UI as

  procedure set_schema_params(p_schema	varchar2,p_git_origin_repo	varchar2,p_subdir	varchar2,p_type_prefix_map	varchar2,
    p_type_suffix_map	varchar2, p_filename_template	varchar2,p_build_system	varchar2) as
  begin
    delete from schema_params
    where schema = p_schema;

    insert into schema_params ( schema,git_origin_repo,subdir,type_prefix_map,type_suffix_map,filename_template,build_system )
    values (p_schema,p_git_origin_repo,p_subdir,p_type_prefix_map,p_type_suffix_map,p_filename_template,p_build_system );

    commit;
  end;

  procedure get_schema_params(p_schema	varchar2,rc_schema_params out sys_refcursor ) as
  begin

   open rc_schema_params for
      select *
      from schema_params
      where schema = p_schema;

  end;

  function list_schema_objects(p_owner varchar2,rc_schema_params out sys_refcursor ) return sys_refcursor
  as
    results sys_refcursor;
  begin

    open rc_schema_params for
      select *
      from schema_params
      where schema = p_owner;

    open results for
      select object_type, owner,object_name, last_ddl_time,sys_context('userenv','DB_NAME') db_name
      from dba_objects where object_type in ( 'PACKAGE','PACKAGE BODY','VIEW',
          'TYPE','TYPE BODY','TABLE','FUNCTION','PROCEDURE','INDEX','SEQUENCE','JOB','TRIGGER','MATERIALIZED VIEW')
          and owner=p_owner
          and (owner, object_name,object_type) not in (
            select owner, object_name,'TABLE'
            from dba_objects
            where owner=p_owner and object_type='MATERIALIZED VIEW'
        );
    return results;
  end;

end admin_ui;
/
