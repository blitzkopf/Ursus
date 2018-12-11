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
    # the directory where the system keeps it clones (/tmp is a really bad choice)
    CloneDirectory = /tmp/clones
    # the branch name used for DB code
    Branch = ursus
    [DATABASE]
    # database to work with can be TNSNAME if set up or full description
    ConnectString = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=<dbhostname>)(PORT=1521))(CONNECT_DATA=(SID=<dbsid>)))
    # username to connect as ( still have not figured out where to keep password)
    Username = URSUS_RUNNER
    # Schema name of installation , should be separate from the user that connects to limit security impact
    Schema = URSUS


## Create the branch 
Do something like 

    git clone /code/oracle/REPO

    git checkout -b ursus
    git pull
    git branch --set-upstream-to=origin/ursus ursus
    git push origin ursus
