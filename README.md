# Ursus

Track Oracle DDL changes in a branch in Git

## Configure daemon

Create a config file

    [GENERAL]
    SourceControlType = GIT
    LogLevel = DEBUG
    # Email domain used for author email address of commit
    EmailDomain = mydomain.com
    [GIT]
    # the directory where the system keeps it repos (/tmp is a really bad choice)
    RepoDirectory = /tmp/repos
    # the branch name used for DB code
    Branch = ursus
    [DATABASE]
    # database to work with can be TNSNAME if set up or full description
    ConnectString = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=<dbhostname>)(PORT=1521))(CONNECT_DATA=(SID=<dbsid>)))
    # username to connect as
    Username = URSUS_RUNNER
    # password to connect with read from ENV variable URSUS_PASSWORD
    Password = ${URSUS_PASSWORD}
    # Schema name of installation , should be separate from the user that connects to limit security impact
    Schema = URSUS

## Configure per schema parameters

    python ./ursusctrl.py --config mirora02.cfg config_schema URSUS --git-origin-repo 'https://tfs.mydomain.com/tfs/big/IT/_git/OracleProject.Git' --subdir=database --filename_template='${schema}/${schema}.${name}.${suffix}' --type_suffix_map=ursus --build_system=liquibase

## Initialize branch with existing code

    python ./ursusctrl.py --config mirora02.cfg init_branch URSUS

## Create the branch

This should be taken care of with init_branch, but you might not want to start with every object commited to git.

Do something like:

    git clone /code/oracle/REPO

    git checkout -b ursus
    git pull
    git branch --set-upstream-to=origin/ursus ursus
    git push origin ursus
