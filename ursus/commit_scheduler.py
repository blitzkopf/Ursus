import time 
from datetime import datetime

class CommitScheduler:
    def __init__(self):
        self.commit_queue = {}
    
    def schedule(self,schema,builder,scheduler,is_valid,schema_params,message,author):
        if (not scheduler or scheduler == 'always'):
                self.commit_queue[schema] = {'commit_time':time.time(),
                     'schema_params':schema_params,'builder':builder,
                     'message':message, 'author':author }

        elif(scheduler == 'inactive' or (scheduler == 'interval' and not self.commit_queue[schema] ) ):
            if(is_valid):
                self.commit_queue[schema] = {'commit_time':time.time()+schema_params.valid_timeout, 
                     'schema_params':schema_params,'builder':builder,
                     'message':message, 'author':author }
            else:
                self.commit_queue[schema] = {'commit_time':time.time()+schema_params.invalid_timeout, 
                     'schema_params':schema_params,'builder':builder,
                     'message':message, 'author':author }
        elif ( scheduler == 'manual' or (scheduler == 'interval' and self.commit_queue[schema] )):
            pass
        else:
            raise Exception("Unknown commit scheduler")

    def cancel(self,schema):
        try:
            del self.commit_queue[schema] 
        except KeyError:
            pass

    def fire(self):
        now = time.time()
        for schema, val in self.commit_queue.copy().items():
            print("Fire!"+schema+ " at "+ str(datetime.fromtimestamp(val['commit_time'])))
            if val['commit_time'] <= now:
                val['builder'].commit(schema,val['schema_params'],val['message'],val['author'])
                del self.commit_queue[schema]