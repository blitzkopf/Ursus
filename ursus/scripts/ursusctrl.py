#!python3
import argparse
import sys
import ursus
import oracledb


class UrsusCtrl(object):
    def __init__(self):

        config,remaining_argv = ursus.init_config(sys.argv)
        oracledb.init_oracle_client()

        self.gitclones = config.get('GIT','CloneDirectory')
        self.gitbranch = config.get('GIT','Branch')
        self.db_connect_string = config.get('DATABASE','ConnectString')
        self.db_username = config.get('DATABASE','Username')
        self.db_schema = config.get('DATABASE','Schema')
        self.email_domain = config.get('GENERAL','EmailDomain')
        self.config=config
        # https://chase-seibert.github.io/blog/2014/03/21/python-multilevel-argparse.html
        parser = argparse.ArgumentParser(
            description='Sets up git for Oracle code',
            usage='''ursusctrl.py <command> [<args>]

Commands:
    config_schema Configure parameters for schema
    init_schema   Create local branch and initialize from schema
    list          Download objects and refs from another repository
''')
        parser.add_argument('command', help='Subcommand to run')
        
        args,remaining_argv  = parser.parse_known_args(remaining_argv)
        
        if not hasattr(self, args.command):
            print('Unrecognized command: '+args.command)
            parser.print_help()
            exit(1)
        # use dispatch pattern to invoke method with same name
        getattr(self, args.command)(remaining_argv)

    def init_branch(self,argv):
        parser = argparse.ArgumentParser(
            description='Set up GIT branch for schema')

        parser.add_argument('schema')
        args = parser.parse_args(argv)

        ddl_handler = ursus.DDLHandler(self.config)
        git_handler = ursus.GITHandler(self.config)

        schema_params = ddl_handler.get_schema_params(args.schema)
        myclone=git_handler.setup_branch(schema_params)
        if schema_params.build_system == 'liquibase':
            builder = ursus.LiquibaseBuilder(self.config,ddl_handler,git_handler)
        elif schema_params.build_system == 'bobcat':
            builder = ursus.BobcatBuilder(config,ddl_handler,git_handler)

        for rec in ddl_handler.list_schema_objects(args.schema):
            print(rec)
            builder.create(rec)
        builder.commit(args.schema,schema_params,"Initial commit from DB","%s <%s@%s>"%('URSUS','ursus',self.email_domain))
        #git_handler.commit_push(myclone,"Initial commit from DB","%s <%s@%s>"%('URSUS','ursus',self.email_domain))
    
    def config_schema(self,argv):
        parser = argparse.ArgumentParser(
            description='Configure schema for use with Ursus')
        parser.add_argument('schema')
        parser.add_argument('--git-origin-repo','--git_origin_repo', '-r' , type=str ,
            dest='git_origin_repo')
        parser.add_argument('--subdir', '-d' , type=str ,
            dest='subdir')
        parser.add_argument('--type-prefix-map','--type_prefix_map', '-p' , type=str ,
            dest='type_prefix_map')
        parser.add_argument('--type-suffix-map', '--type_suffix_map' '-s' , type=str ,
            dest='type_suffix_map')
        parser.add_argument('--filename-template','--filename_template', '-t' , type=str ,
            dest='filename_template')
        parser.add_argument('--build_system', '-b' , type=str ,
            dest='build_system')

        args = parser.parse_args(argv)

        ddl_handler = ursus.DDLHandler(self.config)
        ddl_handler.set_schema_params(args.schema,args)


    def show_schema(self,argv):
        parser = argparse.ArgumentParser(
            description='Configure schema for use with Ursus')
        parser.add_argument('schema')
        args = parser.parse_args(argv)

        ddl_handler = ursus.DDLHandler(self.config)
        schema_params = ddl_handler.get_schema_params(args.schema)
        print(schema_params)


def main():
    UrsusCtrl()

if __name__ == '__main__':
    main()

