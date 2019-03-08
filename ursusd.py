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



def deal_with_it(git_handler,event_data):
    if event_data.sysevent == 'CREATE':
        git_handler.create(event_data)
    elif event_data.sysevent == 'DROP':
        git_handler.drop(event_data)
    elif event_data.sysevent == 'ALTER' and event_data.obj_type == 'TABLE':
        git_handler.alter(event_data)

ddl_handler = ursus.DDLHandler(config)
git_handler = ursus.GITHandler(config,ddl_handler)

while True:
    event_data = ddl_handler.recv_next()
    logging.debug("event_data" + pprint.pformat(event_data))
    if(event_data and event_data.schema_params != None):
        deal_with_it(git_handler,event_data)
    ddl_handler.commit()
