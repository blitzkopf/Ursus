"""Module to handle GIT operations."""

import logging
import os
import subprocess


class GITHandler:
    """Class to handle GIT operations."""

    def __init__(self, config):
        """Initialize the class with the configuration."""
        self.gitrepos = config.get("GIT", "RepoDirectory")
        self.gitbranch = config.get("GIT", "Branch")
        self.email_domain = config.get("GENERAL", "EmailDomain")
        ## FIXME: Don't use fixed name :)
        # self.myrepo = self.gitrepos + os.sep + "Oracle.Git.Poc"

    def reponame(self, schema):
        return self.gitrepos + os.sep + schema

    def commit_push(self, myrepo, message, author, origin_repo=None):
        """Commit and push changes to the GIT repository."""
        os.chdir(myrepo)
        subprocess.call(["git", "commit", "-m", message, "--author", author])
        if origin_repo:
            subprocess.call(["git", "push"])

    def setup_branch(self, schema_params):
        """Clone repository and setup a branch in the GIT repository."""
        logging.debug("changin to repo dir %s" % (self.gitrepos))
        os.chdir(self.gitrepos)
        ##git clone /code/oracle/REPO
        myrepo = self.reponame(schema_params.schema)

        if schema_params.git_origin_repo:
            subprocess.call(["git", "clone", schema_params.git_origin_repo, myrepo])
            logging.debug("changin to  %s" % (myrepo))
            os.chdir(myrepo)
            # git checkout -b ursus
            logging.debug("RUNNING git checkout  -b  %s" % (self.gitbranch))
            subprocess.call(["git", "checkout", "-b", self.gitbranch])
            # git branch --set-upstream-to=origin/ursus ursus
            logging.debug("RUNNING git branch --set-upstream-to=origin/%s %s" % (self.gitbranch, self.gitbranch))
            subprocess.call(["git", "branch", "--set-upstream-to=origin/%s" % (self.gitbranch), self.gitbranch])
            # git pull
            logging.debug("RUNNING git pull")
            subprocess.call(["git", "pull"])
            # git push origin ursus
            subprocess.call(["git", "push", "--set-upstream", "origin", "ursus"])
        else:
            ##git init /code/oracle/REPO
            subprocess.call(["git", "init", myrepo])
            logging.debug("changin to  %s" % (myrepo))
            os.chdir(myrepo)
            # git checkout -b ursus
            logging.debug("RUNNING git checkout  -b  %s" % (self.gitbranch))
            subprocess.call(["git", "checkout", "-b", self.gitbranch])

        return myrepo
