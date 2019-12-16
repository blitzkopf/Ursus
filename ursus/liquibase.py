import re

obj_type_map = {'INDEX':'index','TABLE':'table', 'PACKAGE':'package', 'PACKAGE BODY':'packageBody' }

obj_type_LB_map = {'INDEX':'Index','TABLE':'Table', 'PACKAGE':'Package', 'PACKAGE BODY':'PackageBody' }

class Changelog:
    def __init__(self,filename):
        self.filename = filename
        self.reset_file = False
    
    def  add_step_to_changelog(self,changeset):
        if(not self.reset_file  ):
            try:
                # brute force find end of changelog
                with open(self.filename) as f:
                    ch_lines = f.readlines()
                lastline = ch_lines.pop()
                findend = re.compile(r'(.*)(<\s*/\s*databaseChangeLog\s*>)')
                while True:
                    m = findend.match(lastline)
                    if(m):
                        ch_lines.append(m.group(1))
                        ch_lines.append(changeset)
                        ch_lines.append(m.group(2))
                        break
                    lastline = ch_lines.pop()
            except IOError:
                self.reset_file=True
        if( self.reset_file  ):
            ch_lines = [
                '''<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog
  xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
         http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd">
''',changeset,'</databaseChangeLog>'
            ]

        with open(self.filename,'w') as f:
            for line in ch_lines:
                f.write(line)
        self.reset_file = False
    
    def generate_changeset(self,event_type,author,id,schema,obj_name,obj_type,statement=None,filename=None,delimiter=';',precondition=''):
        if statement and filename:
            raise Exception("Can not specify both filename and statement {} {}".format(filename,statement))
        if(statement):
            changeset = '''
    <changeSet author="{}" id="{}">
        {}
        <sql endDelimiter="{}"
            splitStatements="true" 
            stripComments="false">
            {}
        </sql>
    </changeSet>
            '''.format(author,id,precondition,delimiter,statement)
        elif (filename):
            if(obj_type == 'TABLE'):
                run_on_change = 'false'
            else:
                run_on_change = 'true' 
            changeset = '''
    <changeSet author="{}"
        id="{}"
        objectQuotingStrategy="LEGACY"
        runOnChange="true">
        {}
        <sqlFile 
            encoding="UTF-8"
            path="{}"
            relativeToChangelogFile="true"
            endDelimiter="{}"
            splitStatements="true"
            stripComments="false"/>
    </changeSet>\n'''.format(author,id,precondition,filename,run_on_change,delimiter)
        return changeset
    def get_precond(self,obj_name,object_type,check_if_exists=True):
        try:
            pc_prefix = obj_type_map[object_type]
        except KeyError:
            return ''
        if(check_if_exists):
            return '''
    <preConditions onFail="MARK_RAN">
        <not><{0}Exists  {0}Name="{1}" /></not>
    </preConditions>
'''.format(pc_prefix,obj_name)
        else:
            return '''
    <preConditions onFail="MARK_RAN">
        <{0}Exists  {0}Name="{1}" />
    </preConditions>
'''.format(pc_prefix,obj_name)


    def add_to_changelog(self,event_type,author,id,schema,obj_name,obj_type,statement=None,filename=None,delimiter=';',precondition=''):
        chset = self.generate_changeset(event_type,author,id,schema,obj_name,obj_type,statement,filename,delimiter,precondition)
        self.add_step_to_changelog(chset)

