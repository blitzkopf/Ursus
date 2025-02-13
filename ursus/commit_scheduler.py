"""Schedules commits based on the schema's scheduler parameter.

In some cases we don't want to commit changes immediately, but rather wait for a certain time to see
if there are any more changes.
This class is responsible for scheduling commits based on the schema's scheduler parameter.
The scheduler parameter can be one of the following values:
- always: Commit changes immediately
- inactive: Do not commit changes unless the schema is inactive for specified time
- interval: Commit changes after a certain time interval, even if the user is still active.
- manual: User has requested a manual commit.

There can be diffrent time intervals for valid and invalid changes.
We might want to delay commiting invalid changes for longer in hope that the user will fix them.

The scheduler parameters is set in the SCHEMA_PARAMSA configuration table.
"""

import time
from datetime import datetime


class CommitScheduler:
    """Class to schedule commits based on the schema's scheduler parameters."""

    def __init__(self):
        """Initialize the class with an empty commit queue."""
        self.commit_queue = {}

    def schedule(self, schema, builder, scheduler, is_valid, schema_params, message, author):
        """Schedule a commit based on the parameters. Or run it immediately if scheduler is always."""
        if not scheduler or scheduler == "always":
            self.commit_queue[schema] = {
                "commit_time": time.time(),
                "schema_params": schema_params,
                "builder": builder,
                "message": message,
                "author": author,
            }

        elif scheduler == "inactive" or (scheduler == "interval" and not self.commit_queue[schema]):
            if is_valid:
                self.commit_queue[schema] = {
                    "commit_time": time.time() + schema_params.valid_timeout,
                    "schema_params": schema_params,
                    "builder": builder,
                    "message": message,
                    "author": author,
                }
            else:
                self.commit_queue[schema] = {
                    "commit_time": time.time() + schema_params.invalid_timeout,
                    "schema_params": schema_params,
                    "builder": builder,
                    "message": message,
                    "author": author,
                }
        elif scheduler == "manual" or (scheduler == "interval" and self.commit_queue[schema]):
            pass
        else:
            raise Exception("Unknown commit scheduler")

    def cancel(self, schema):
        """Cancel a scheduled commit."""
        try:
            del self.commit_queue[schema]
        except KeyError:
            pass

    def fire(self):
        """Fire the scheduled commits if their time is up."""
        now = time.time()
        for schema, val in list(self.commit_queue.items()):
            print("Fire!" + schema + " at " + str(datetime.fromtimestamp(val["commit_time"])))
            if val["commit_time"] <= now:
                val["builder"].commit(schema, val["schema_params"], val["message"], val["author"])
                del self.commit_queue[schema]
