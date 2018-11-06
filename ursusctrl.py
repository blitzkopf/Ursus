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

config,remaining_argv = ursus.init_config(sys.argv)

gitclones = config.get('GIT','CloneDirectory')
gitbranch = config.get('GIT','Branch')
db_connect_string = config.get('DATABASE','ConnectString')
db_username = config.get('DATABASE','Username')
db_schema = config.get('DATABASE','Schema')
email_domain = config.get('GENERAL','EmailDomain')

ddl_handler = ursus.DDLHandler(config)
git_handler = ursus.GITHandler(config,ddl_handler)

for rec in ddl_handler.list_schema_objects('YNGVI'):
    print(rec)
    git_handler.create(rec)

