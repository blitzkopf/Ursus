"""# This is the main daemon script that listens for AQ events and processes them."""

import logging
import pprint
import sys

import cysystemd.daemon
import oracledb

import ursus

LOGGER = logging.getLogger(__name__)


def manual_commit(builder, event_data, commit_scheduler, email_domain):
    """Commit the changes to the git repository when the user has requested it."""
    builder.commit(
        event_data.obj_owner,
        event_data.schema_params,
        event_data.sql_text,
        "%s <%s@%s>" % (event_data.os_user, event_data.os_user, email_domain),
    )
    commit_scheduler.cancel(event_data.obj_owner)


def deal_with_it(builder, event_data, commit_scheduler, email_domain):
    """Decide what to do with the event data."""
    schema_params = event_data.schema_params

    if event_data.obj_status == "VALID":
        is_valid = True
    else:
        is_valid = False
    if event_data.sysevent == "CREATE":
        builder.create(event_data)
    elif event_data.sysevent == "DROP":
        builder.drop(event_data)
    elif event_data.sysevent == "ALTER" and event_data.obj_type == "TABLE":
        builder.alter(event_data)
    elif event_data.obj_type == "OBJECT PRIVILEGE":
        builder.grant_revoke(event_data)
    elif event_data.sysevent == "GIT_COMMIT":
        manual_commit(builder, event_data, commit_scheduler, email_domain)
        return
    else:
        return
    commit_scheduler.schedule(
        event_data.obj_owner,
        builder,
        schema_params.commit_behavior,
        is_valid,
        schema_params,
        "Automatic for the people (%s %s.%s (%s)) "
        % (event_data.sysevent, event_data.obj_owner, event_data.obj_name, event_data.obj_type),
        "%s <%s@%s>" % (event_data.os_user, event_data.os_user, email_domain),
    )


def main():
    config, remaining_argv = ursus.init_config(sys.argv)
    oracledb.init_oracle_client()

    assert config.get("GIT", "RepoDirectory"), "RepoDirectory must be set in the configuration file"
    assert config.get("GIT", "Branch"), "Branch must be set in the configuration file"
    assert config.get("DATABASE", "ConnectString"), "ConnectString must be set in the configuration file"
    assert config.get("DATABASE", "Username"), "Username must be set in the configuration file"
    assert config.get("DATABASE", "Schema"), "Schema must be set in the configuration file"
    assert config.get("GENERAL", "EmailDomain"), "EmailDomain must be set in the configuration file"
    email_domain = config.get("GENERAL", "EmailDomain")

    ddl_handler = ursus.DDLHandler(config)
    git_handler = ursus.GITHandler(config)
    bobcatbuilder = ursus.BobcatBuilder(config, ddl_handler, git_handler)
    liquibasebuilder = ursus.LiquibaseBuilder(config, ddl_handler, git_handler)
    commit_scheduler = ursus.CommitScheduler()
    # cysystemd.daemon.notify(cysystemd.daemon.Notification.READY)
    """Main loop for the daemon, listens for AQ events and processes them."""
    while True:
        event_data = ddl_handler.recv_next()
        LOGGER.debug("event_data:" + pprint.pformat(event_data))
        if event_data and event_data.schema_params is not None:
            if event_data.schema_params.build_system == "bobcat":
                builder = bobcatbuilder
            elif event_data.schema_params.build_system == "liquibase":
                builder = liquibasebuilder
            deal_with_it(builder, event_data, commit_scheduler, email_domain)
        ddl_handler.commit()
        commit_scheduler.fire()
