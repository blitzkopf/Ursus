#!python3
import traceback
from string import Template
import os
import os.path
import subprocess
import sys
import ursus
import logging
import pprint

config = ursus.init_config(sys.argv)

gitclones = config.get('GIT','CloneDirectory')
gitbranch = config.get('GIT','Branch')
db_connect_string = config.get('DATABASE','ConnectString')
db_username = config.get('DATABASE','Username')
db_schema = config.get('DATABASE','Schema')
email_domain = config.get('GENERAL','EmailDomain')


filename_template = Template('database/${schema}/${schema}.${name}.${suffix}')
myclone = gitclones + os.sep + "AQ_DDL"

extension_map = {
	'PACKAGE':'PAK',
	'PACKAGE BODY':'PLS',
	'VIEW':'VIW',
	'TABLE':'TAB',
	'TYPE':'TYP',
	'TYPE BODY':'TPB',
	'PROCEDURE':'SQL',
	'FUNCTION':'SQL',
	'INDEX':'IDX',
	'SEQUENCE':'SEQ',
	'JOB':'JOB',
	'TRIGGER':'TRI',
   }

def get_fullname_clonedir(event_data):
    if event_data.schema_params.type_suffix_map :
        suffix = ddl_handler.map(event_data.schema_params.type_suffix_map,event_data.obj_type,event_data.obj_type)
    else: 
        suffix = ''
    if event_data.schema_params.type_prefix_map :
        prefix = ddl_handler.map(event_data.schema_params.type_prefix_map,event_data.obj_type,event_data.obj_type)
    else: 
        prefix = ''
    #suffix = extension_map.get(event_data.obj_type, event_data.obj_type)

    #filename = filename_template.substitute(schema=event_data.obj_owner, name=event_data.obj_name,type = event_data.obj_type, suffix=suffix)
    filename = Template(event_data.schema_params.filename_template).substitute(schema=event_data.obj_owner, name=event_data.obj_name,
            type = event_data.obj_type, suffix=suffix,prefix=prefix)
    #fullname = myclone + os.sep + filename
    fullname = myclone + os.sep + event_data.schema_params.subdir + os.sep + filename
    ## TODO find clone dir
    return fullname,myclone 
    
def git_create(ddl_handler,event_data):
    fullname,myclone=get_fullname_clonedir(event_data)
    dir = os.path.dirname(fullname)
    if not os.path.exists(dir) :
        os.makedirs(dir)
    ##if not os.path.isdir(dir):
    ##    raise  

    logging.info("committing file "+fullname)
    
    os.chdir(myclone)

    with  open(fullname, 'w') as f:
        for line in ddl_handler.get_source_lines(event_data.obj_owner,event_data.obj_name,event_data.obj_type):
            f.write(line)
    subprocess.call( [ "git",  "stage", fullname] ) 
    subprocess.call( [ "git",  "commit", 
        "-m", "Automatic for the people (%s %s.%s (%s)) "% (event_data.sysevent,event_data.obj_owner,event_data.obj_name,event_data.obj_type ) , 
        '--author',   "%s <%s@%s>"%(event_data.os_user,event_data.os_user,email_domain)] )
    subprocess.call( [ "git",  "push" ] )

def git_drop(ddl_handler,event_data):
    fullname,myclone=get_fullname_clonedir(event_data)
    dir = os.path.dirname(fullname)
    if not os.path.exists(dir) :
        os.makedirs(dir)
    ##if not os.path.isdir(dir):
    ##    raise  

    logging.info("removing file "+fullname)
    
    os.chdir(myclone)

    subprocess.call( [ "git",  "rm", fullname] ) 
    subprocess.call( [ "git",  "commit", 
        "-m", "Automatic for the people (%s %s.%s (%s)) "% (event_data.sysevent,event_data.obj_owner,event_data.obj_name,event_data.obj_type ) , 
        '--author',   "%s <%s@%s>"%(event_data.os_user,event_data.os_user,email_domain)] )
    subprocess.call( [ "git",  "push" ] )


def deal_with_it(ddl_handler,event_data):
    if event_data.sysevent == 'CREATE':
        git_create(ddl_handler,event_data)
    elif event_data.sysevent == 'DROP':
        git_drop(ddl_handler,event_data)
    
        
    

ddl_handler = ursus.DDLHandler(config)

while True:
    event_data = ddl_handler.recv_next()
    logging.debug("event_data" + pprint.pformat(event_data))
    if(event_data and event_data.schema_params != None):
        deal_with_it(ddl_handler,event_data)
    ddl_handler.commit()
