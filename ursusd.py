#!python3
import traceback
import os
import os.path
import sys
import ursus
import logging
import pprint

config,remaining_argv = ursus.init_config(sys.argv)

gitclones = config.get('GIT','CloneDirectory')
gitbranch = config.get('GIT','Branch')
db_connect_string = config.get('DATABASE','ConnectString')
db_username = config.get('DATABASE','Username')
db_schema = config.get('DATABASE','Schema')
email_domain = config.get('GENERAL','EmailDomain')

ddl_handler = ursus.DDLHandler(config)
git_handler = ursus.GITHandler(config,ddl_handler)
commit_scheduler = ursus.CommitScheduler(git_handler)

def deal_with_it(git_handler,event_data):
    schema_params = event_data.schema_params
    if(event_data.obj_status == 'VALID'):
        is_valid = True
    else:
        is_valid = False
    if event_data.sysevent == 'CREATE':
        git_handler.create(event_data)
    elif event_data.sysevent == 'DROP':
        git_handler.drop(event_data)
    elif event_data.sysevent == 'ALTER' and event_data.obj_type == 'TABLE':
        git_handler.alter(event_data)
    else:
        return
    commit_scheduler.schedule(event_data.obj_owner,schema_params.commit_behavior,is_valid,schema_params,
        "Automatic for the people (%s %s.%s (%s)) "% (event_data.sysevent,event_data.obj_owner,event_data.obj_name,event_data.obj_type ),
        "%s <%s@%s>"%(event_data.os_user,event_data.os_user,email_domain))
    

while True:
    event_data = ddl_handler.recv_next()
    logging.debug("event_data" + pprint.pformat(event_data))
    if(event_data and event_data.schema_params != None):
        deal_with_it(git_handler,event_data)
    ddl_handler.commit()
    commit_scheduler.fire()
