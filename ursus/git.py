
from string import Template
import subprocess
import os
import logging

class GITHandler:
    def __init__(self,config,ddl_handler):
        self.gitclones = config.get('GIT','CloneDirectory')
        self.gitbranch = config.get('GIT','Branch')
        self.email_domain = config.get('GENERAL','EmailDomain')
        self.myclone = self.gitclones + os.sep + "AQ_DDL"
        self.ddl_handler = ddl_handler

    def get_fullname_clonedir(self,event_data):
        if event_data.schema_params.type_suffix_map :
            suffix = self.ddl_handler.map(event_data.schema_params.type_suffix_map,event_data.obj_type,event_data.obj_type)
        else: 
            suffix = ''
        if event_data.schema_params.type_prefix_map :
            prefix = self.ddl_handler.map(event_data.schema_params.type_prefix_map,event_data.obj_type,event_data.obj_type)
        else: 
            prefix = ''
        #suffix = extension_map.get(event_data.obj_type, event_data.obj_type)

        filename = Template(event_data.schema_params.filename_template).substitute(schema=event_data.obj_owner, name=event_data.obj_name,
                type = event_data.obj_type, suffix=suffix,prefix=prefix)
        #fullname = myclone + os.sep + filename
        fullname = self.myclone + os.sep + event_data.schema_params.subdir + os.sep + filename
        ## TODO find clone dir
        return fullname,self.myclone 
        
    def create(self,event_data):
        fullname,myclone=self.get_fullname_clonedir(event_data)
        dir = os.path.dirname(fullname)
        if not os.path.exists(dir) :
            os.makedirs(dir)
        ##if not os.path.isdir(dir):
        ##    raise  

        logging.info("committing file "+fullname)
        
        os.chdir(myclone)

        with  open(fullname, 'w') as f:
            for line in self.ddl_handler.get_source_lines(event_data.obj_owner,event_data.obj_name,event_data.obj_type):
                f.write(line)
        subprocess.call( [ "git",  "stage", fullname] ) 
        subprocess.call( [ "git",  "commit", 
            "-m", "Automatic for the people (%s %s.%s (%s)) "% (event_data.sysevent,event_data.obj_owner,event_data.obj_name,event_data.obj_type ) , 
            '--author',   "%s <%s@%s>"%(event_data.os_user,event_data.os_user,self.email_domain)] )
        subprocess.call( [ "git",  "push" ] )

    def drop(self,event_data):
        fullname,myclone=self.get_fullname_clonedir(event_data)
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
            '--author',   "%s <%s@%s>"%(event_data.os_user, event_data.os_user, self.email_domain)] )
        subprocess.call( [ "git",  "push" ] )
