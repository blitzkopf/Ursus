"""Buildsystems module.

Structures files in a git repository to be used by a build system.
"""

import json
import logging
import os
import os.path
import subprocess
import time
from datetime import datetime
from string import Template

## ULGY: need to remove this dependency on oracle
import oracledb
import yaml

from .liquibase import Changelog

_LOG = logging.getLogger(__name__)


class Builder(object):
    """Base class for builders."""

    def __init__(self, config, ddl_handler, git_handler):
        """Initialize the builder."""
        self.gitrepos = config.get("GIT", "RepoDirectory")
        self.gitbranch = config.get("GIT", "Branch")
        self.email_domain = config.get("GENERAL", "EmailDomain")
        ## FIXME: Don't use fixed name :)
        ## self.myrepo = self.gitrepos + os.sep + "Oracle.Git.Poc"
        self.ddl_handler = ddl_handler
        self.git_handler = git_handler

    def get_fullname_repodir(self, event_data):
        """Get the full name of the file and the repo directory."""
        if event_data.schema_params.type_suffix_map:
            suffix = self.ddl_handler.map(
                event_data.schema_params.type_suffix_map, event_data.obj_type, event_data.obj_type
            )
        else:
            suffix = ""
        if event_data.schema_params.type_prefix_map:
            prefix = self.ddl_handler.map(
                event_data.schema_params.type_prefix_map, event_data.obj_type, event_data.obj_type
            )
        else:
            prefix = ""
        # suffix = extension_map.get(event_data.obj_type, event_data.obj_type)

        filename = Template(event_data.schema_params.filename_template).substitute(
            schema=event_data.obj_owner,
            name=event_data.obj_name,
            type=event_data.obj_type,
            suffix=suffix,
            prefix=prefix,
        )
        # fullname = myclone + os.sep + filename
        myrepo = self.git_handler.reponame(event_data.schema_params.schema)
        fullname = myrepo + os.sep + event_data.schema_params.subdir + os.sep + filename
        ## TODO find clone dir
        return fullname, myrepo

    def create(self, event_data, file_header=None, file_footer=None):
        """Generate a file for object CREATE statement."""
        fullname, myrepo = self.get_fullname_repodir(event_data)
        dir = os.path.dirname(fullname)
        if not os.path.exists(dir):
            os.makedirs(dir)
        ##if not os.path.isdir(dir):
        ##    raise

        logging.info("committing file " + fullname)

        os.chdir(myrepo)
        try:
            with open(fullname, "w") as f:
                if file_header:
                    f.write(file_header)
                for line in self.ddl_handler.get_source_lines(
                    event_data.obj_owner, event_data.obj_name, event_data.obj_type
                ):
                    f.write(line)
                if file_footer:
                    f.write(file_footer)
            subprocess.call(["git", "stage", fullname])
        except oracledb.DatabaseError as e:
            (oerr,) = e.args
            logging.error("ORA-" + str(oerr.code) + " " + event_data.obj_type + " " + (event_data.obj_name[:6]))
            if oerr.code == 31603 and event_data.obj_type == "SEQUENCE" and event_data.obj_name[:6] == "ISEQ$$":
                logging.info(event_data.obj_name + " is a identity column sequence will be skipped")
                os.remove(fullname)
            elif oerr.code == 31603:
                logging.error(event_data.obj_name + " could not be found, probably already dropped")
                os.remove(fullname)
            else:
                raise
        return fullname

    def alter(self, event_data):
        """Generate a file for object ALTER statement."""
        self.create(event_data)

    def drop(self, event_data):
        """Generate a file for object DROP statement."""
        fullname, myrepo = self.get_fullname_repodir(event_data)
        dir = os.path.dirname(fullname)
        if not os.path.exists(dir):
            os.makedirs(dir)
        ##if not os.path.isdir(dir):
        ##    raise

        logging.info("removing file " + fullname)

        os.chdir(myrepo)

        subprocess.call(["git", "rm", fullname])

    def prepare_pri_file(self, owner, schema_params):
        """Prepare the priority file for the schema.

        Priority file is used to determine the order of the files in the build system.
        """
        dependency_priority = self.ddl_handler.get_depend_priority(
            owner, schema_params.type_prefix_map, schema_params.type_suffix_map
        )
        myrepo = self.git_handler.reponame(schema_params.schema)
        pri_file = myrepo + os.sep + schema_params.subdir + os.sep + ".ursus_dependency_priority"
        try:
            with open(pri_file) as json_file:
                pri_dict = json.load(json_file)
        except IOError:
            pri_dict = {}
        for obj in dependency_priority:
            filename = Template(schema_params.filename_template).substitute(
                schema=obj["d_owner"],
                name=obj["d_name"],
                type=obj["d_type"],
                prefix=obj["type_mapped1"],
                suffix=obj["type_mapped2"],
            )
            print(filename)
            pri_dict[filename] = obj["max_level"]
        with open(pri_file, "w") as outfile:
            json.dump(pri_dict, outfile, sort_keys=True, indent=2)
        subprocess.call(["git", "stage", pri_file])
        return pri_dict

    def commit(self, owner, schema_params, message, author):
        """Commit the changes to the git repository."""
        myrepo = self.git_handler.reponame(schema_params.schema)
        self.git_handler.commit_push(myrepo, message, author, schema_params.git_origin_repo)


class BobcatBuilder(Builder):
    """Bobcat builder class."""

    def __init__(self, config, ddl_handler, git_handler):
        """Initialize the builder."""
        super(BobcatBuilder, self).__init__(config, ddl_handler, git_handler)

    def alter(self, event_data):
        """Generate a file for object ALTER statement."""
        fullname, myrepo = self.get_fullname_repodir(event_data)
        (dir, filename) = os.path.split(fullname)
        dir = os.path.join(dir, "changes")
        if not os.path.exists(dir):
            os.makedirs(dir)
        fullname = os.path.join(dir, filename)
        ##if not os.path.isdir(dir):
        ##    raise
        logging.info("alter file " + fullname)

        os.chdir(myrepo)
        with open(fullname, "a+") as f:
            f.write("-- %s, change by: %s\n" % (datetime.now(), event_data.os_user))
            f.write(event_data.sql_text)
            f.write(";\n")
        subprocess.call(["git", "stage", fullname])
        super(BobcatBuilder, self).alter(event_data)

    def commit(self, owner, schema_params, message, author):
        """Commit the changes to the git repository."""
        self.prepare_pri_file(owner, schema_params)
        super(BobcatBuilder, self).commit(owner, schema_params, message, author)


class LiquibaseBuilder(Builder):
    """Liquibase builder class."""

    def __init__(self, config, ddl_handler, git_handler):
        """Initialize the builder."""
        super(LiquibaseBuilder, self).__init__(config, ddl_handler, git_handler)

    def get_master_chlog_file(self, schema_params):
        """Get the master changelog file."""
        myrepo = self.git_handler.reponame(schema_params.schema)
        return myrepo + os.sep + schema_params.subdir + os.sep + "Changelog.yaml"

    def create(self, event_data):
        """Generate a file for object CREATE statement."""
        fullname = super(LiquibaseBuilder, self).create(event_data)
        (dir, filename) = os.path.split(fullname)
        changelog_file = fullname + ".yaml"
        chlog = Changelog(changelog_file)

        delimiter = self.ddl_handler.get_delimiter(event_data.obj_type)
        if event_data.obj_type == "INDEX" and self.ddl_handler.is_constraint_index(
            event_data.obj_owner, event_data.obj_name, event_data.obj_type
        ):
            precond = chlog.get_precond(event_data.obj_name, "INDEX")
        elif event_data.obj_type == "TABLE":
            precond = chlog.get_precond(event_data.obj_name, "TABLE")
        else:
            precond = ""
        chlog.add_to_changelog(
            "CREATE",
            event_data.os_user,
            time.time(),
            event_data.obj_owner,
            event_data.obj_name,
            event_data.obj_type,
            filename=filename,
            delimiter=delimiter,
            precondition=precond,
        )
        subprocess.call(["git", "stage", fullname + ".yaml"])

    def alter(self, event_data):
        """Generate a file for object ALTER statement."""
        super(LiquibaseBuilder, self).create(event_data)  ## call to keep file in sync

        fullname, myrepo = self.get_fullname_repodir(event_data)
        (dir, filename) = os.path.split(fullname)
        changelog_file = fullname + ".yaml"
        chlog = Changelog(changelog_file)

        logging.info("alter file " + changelog_file)
        delimiter = self.ddl_handler.get_delimiter(event_data.obj_type)
        if delimiter == "/":
            delimiter = r"^\s*/\s*$"
        chlog.add_to_changelog(
            "ALTER",
            event_data.os_user,
            time.time(),
            event_data.obj_owner,
            event_data.obj_name,
            event_data.obj_type,
            statement=event_data.sql_text,
            delimiter=delimiter,
        )
        # os.chdir(myclone)
        subprocess.call(["git", "stage", changelog_file])
        # super(BobcatBuilder,self).alter(event_data)

    def drop(self, event_data):
        """Generate a file for object DROP statement."""
        super(LiquibaseBuilder, self).drop(event_data)  ## call to keep file in sync
        fullname, myrepo = self.get_fullname_repodir(event_data)
        changelog_file = fullname + ".yaml"
        chlog = Changelog(changelog_file)
        chlog.reset_file = True
        logging.info("dropping " + changelog_file)
        precond = chlog.get_precond(event_data.obj_name, event_data.obj_type, False)
        chlog.add_to_changelog(
            "DROP",
            event_data.os_user,
            time.time(),
            event_data.obj_owner,
            event_data.obj_name,
            event_data.obj_type,
            statement=event_data.sql_text,
            precondition=precond,
        )

        subprocess.call(["git", "stage", changelog_file])

    def commit(self, owner, schema_params, message, author):
        """Commit the changes to the git repository."""
        pri_dict = self.prepare_pri_file(owner, schema_params)
        files = []
        db_code_dir = schema_params.subdir
        changelog_filename = self.get_master_chlog_file(schema_params)
        _LOG.debug("changelog file:{}".format(changelog_filename))
        _LOG.debug("spooling files {}".format(os.getcwd()))
        for dirpath, _dirnames, filenames in os.walk(db_code_dir):  # do we need the subdir from filename_template?
            prefix = os.path.relpath(dirpath, db_code_dir)
            _LOG.debug("path:{}".format(dirpath))
            for file in filenames:
                relname = os.path.join(prefix, file)
                _LOG.debug("file:{} ".format(relname))
                (basename, extension) = os.path.splitext(relname)
                if dirpath == db_code_dir and file == "Changelog.yaml":
                    continue
                if extension == ".yaml":
                    pri = pri_dict.get(basename, 0)
                    _LOG.debug("file:{} priority:{}".format(relname, pri))
                    files.append({"filename": relname, "priority": pri})
        includes = []
        for file in sorted(files, key=lambda f: f["priority"]):
            includes.append({"include": {"relativeToChangelogFile": True, "file": file["filename"]}})

        with open(changelog_filename, "w") as f:
            f.write(yaml.dump({"databaseChangeLog": includes}))

        super(LiquibaseBuilder, self).commit(owner, schema_params, message, author)
