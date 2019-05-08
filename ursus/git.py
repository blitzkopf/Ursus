
from string import Template
import subprocess
import os
import logging
import json
from datetime import datetime
## ULGY: need to remove this dependency on oracle 
import cx_Oracle

class GITHandler:
    def __init__(self,config,ddl_handler):
        self.gitclones = config.get('GIT','CloneDirectory')
        self.gitbranch = config.get('GIT','Branch')
        self.email_domain = config.get('GENERAL','EmailDomain')
        ## FIXME: Don't use fixed name :)
        self.myclone = self.gitclones + os.sep + "Oracle.Git.Poc"
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
        try:
            with  open(fullname, 'w') as f:
                for line in self.ddl_handler.get_source_lines(event_data.obj_owner,event_data.obj_name,event_data.obj_type):
                    f.write(line)
            subprocess.call( [ "git",  "stage", fullname] ) 
        except cx_Oracle.DatabaseError as e:
            oerr, = e.args
            logging.error("ORA-"+str(oerr.code)+" "+event_data.obj_type+" "+(event_data.obj_name[:6]))
            if(oerr.code == 31603 and event_data.obj_type == 'SEQUENCE' and event_data.obj_name[:6] == 'ISEQ$$' ):
                logging.info(event_data.obj_name+" is a identity column sequence will be skipped")
                os.remove(fullname)
            elif(oerr.code == 31603 ):
                logging.error(event_data.obj_name+" could not be found, probably already dropped")
                os.remove(fullname)
            else:
                raise

    def commit(self,owner,schema_params,message,author):
        dependency_priority = self.ddl_handler.get_depend_priority(owner,schema_params.type_prefix_map,schema_params.type_suffix_map)
        pri_file = self.myclone + os.sep + schema_params.subdir + os.sep + '.ursus_dependency_priority'
        try:
            with open(pri_file) as json_file:  
                pri_dict = json.load(json_file)
        except IOError:
            pri_dict={}
        for obj in dependency_priority:
            filename = Template(schema_params.filename_template).substitute(schema=obj['d_owner'], name=obj['d_name'],
                type = obj['d_type'], prefix=obj['type_mapped1'],suffix=obj['type_mapped2'])
            print(filename)
            pri_dict[filename] = obj['max_level']
        with open(pri_file, 'w') as outfile:
            json.dump(pri_dict, outfile,sort_keys=True, indent=2) 
        subprocess.call( [ "git",  "stage", pri_file] ) 
        self.commit_push(self.myclone,message,author )       

            

    def commit_push(self,myclone,message,author):
        os.chdir(myclone)
        subprocess.call( [ "git",  "commit", 
            "-m", message , 
            '--author',   author] )
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
    
    def alter(self,event_data):
        fullname,myclone=self.get_fullname_clonedir(event_data)
        (dir,filename) = os.path.split(fullname)
        dir=os.path.join(dir,'changes')
        if not os.path.exists(dir) :
            os.makedirs(dir)
        fullname = os.path.join(dir , filename)
        ##if not os.path.isdir(dir):
        ##    raise  
        logging.info("alter file "+fullname)

        os.chdir(myclone)
        with  open(fullname, 'a+') as f:
            f.write("-- %s, change by: %s\n"%(datetime.now(),event_data.os_user))
            f.write(event_data.sql_text)
            f.write(';\n')
        subprocess.call( [ "git",  "stage", fullname] ) 
        self.create(event_data)
    

    def setup_branch(self,schema_params):
        logging.debug("changin to clone dir %s"%(self.gitclones))
        os.chdir(self.gitclones)
        ##git clone /code/oracle/REPO
        subprocess.call( [ "git",  "clone", schema_params.git_origin_repo] ) 
        logging.debug("changin to  %s"%(self.myclone))
        os.chdir(self.myclone)
        #git checkout -b ursus
        logging.debug("RUNNING git checkout  -b  %s"%(self.gitbranch))
        subprocess.call( [ "git",  "checkout", "-b", self.gitbranch] ) 
        #git branch --set-upstream-to=origin/ursus ursus
        logging.debug("RUNNING git branch --set-upstream-to=origin/%s %s"%(self.gitbranch,self.gitbranch ))
        subprocess.call( [ "git",  "branch", "--set-upstream-to=origin/%s"%(self.gitbranch), self.gitbranch ] ) 
        #git pull
        logging.debug("RUNNING git pull")
        subprocess.call( [ "git",  "pull" ] ) 
        #git push origin ursus
        subprocess.call( [ "git",  "push", "--set-upstream", "origin", "ursus" ] ) 
        return self.myclone
