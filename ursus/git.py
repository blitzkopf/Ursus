from string import Template
import subprocess
import os
import logging
import json
from datetime import datetime

class GITHandler:
    def __init__(self,config):
        self.gitclones = config.get('GIT','CloneDirectory')
        self.gitbranch = config.get('GIT','Branch')
        self.email_domain = config.get('GENERAL','EmailDomain')
        ## FIXME: Don't use fixed name :)
        self.myclone = self.gitclones + os.sep + "Oracle.Git.Poc"
        
    def commit_push(self,myclone,message,author):
        os.chdir(myclone)
        subprocess.call( [ "git",  "commit", 
            "-m", message , 
            '--author',   author] )
        subprocess.call( [ "git",  "push" ] )
            
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
