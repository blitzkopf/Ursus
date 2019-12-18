import re
import xmltodict
import yaml
import pprint

obj_type_map = {'INDEX':'index','TABLE':'table', 'PACKAGE':'package', 'PACKAGE BODY':'packageBody' }

obj_type_LB_map = {'INDEX':'Index','TABLE':'Table', 'PACKAGE':'Package', 'PACKAGE BODY':'PackageBody' }

class Changelog:
    def __init__(self,filename):
        self.filename = filename
        self.reset_file = False
        self.changesets=[]
        self.format = 'xml'
    
    def read_changelog(self):
        chl_string = ''
        try:
            with open(self.filename) as f:
                for line in f.readlines():
                    chl_string += line
        except IOError:
            return
        if(self.format=='xml'):
            chl_dict = xmltodict.parse(chl_string)
            print(chl_dict)
            chset = chl_dict['databaseChangeLog']['changeSet']
            if(type(chset) == list):
                self.changesets = [{'changeSet':x} for x in chset]
            else:
                print("Failed!")
                self.changesets = [{'changeSet':chset}]
            pprint.pprint(self.changesets)
        elif(self.format=='yaml'):
            chl_dict = yaml.parse(chl_string)
    
    def write_changelog(self):
        if(self.format=='xml'):
            chl_string = xmltodict.unparse(
                {'databaseChangeLog':{
                 '@xmlns':"http://www.liquibase.org/xml/ns/dbchangelog",
                '@xmlns:xsi':"http://www.w3.org/2001/XMLSchema-instance",
                '@xsi:schemaLocation':"http://www.liquibase.org/xml/ns/dbchangelog,http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd",
                'changeSet':[cs['changeSet'] for cs in self.changesets]
                }},pretty=True)
        elif(self.format=='yaml'):
            chl_string = xmltodict.unparse({'databaseChangeLog':self.changesets})
            chl_dict = yaml.parse(chl_string)
        with open(self.filename,'w') as f:
            f.write(chl_string )

    def get_attr_prefix(self):
        if(self.format=='xml'):
            return '@'
        else:
            return ''

    def get_text_key(self,element):
        if(self.format=='xml'):
            return '#text'
        elif(self.format=='yaml'):
            return element

    def  add_step_to_changelog(self,changeset):
        if(not self.reset_file  ):
            self.read_changelog()
            self.changesets.append(changeset)
        if( self.reset_file  ):
            self.changesets=[changeset]
        self.write_changelog()
        self.reset_file = False
    
    def generate_changeset(self,event_type,author,id,schema,obj_name,obj_type,statement=None,filename=None,delimiter=';',precondition=None):
        if statement and filename:
            raise Exception("Can not specify both filename and statement {} {}".format(filename,statement))
        at=self.get_attr_prefix()
        if(statement):
            changeset = {'changeSet':{ 
                at+'id':id,
                at+'author':author,
                'changes':[
                    {'sql': {
                        at+'endDelimiter':delimiter,
                        at+'splitStatements':True ,
                        at+'stripComments':False,
                        self.get_text_key('sql'): statement
                    }
                }
                ]
            }}
        elif (filename):
            if(obj_type == 'TABLE'):
                run_on_change = False
            else:
                run_on_change = True
            changeset  = {'changeSet':{ 
                at+'id':id,
                at+'author':author,
                at+'runOnChange':run_on_change,
                'changes':[
                    {'sqlFile': {
                        at+'endDelimiter':delimiter,
                        at+'splitStatements':True ,
                        at+'stripComments':False,
                        at+'relativeToChangelogFile':True,
                        at+'path':filename,
                        self.get_text_key('sql'): statement
                    }}
                ]
            }}
        if(precondition):
            changeset['changeSet']['preCondition']=precondition['preCondition'] 
        return changeset
    
    def get_precond(self,obj_name,object_type,check_if_exists=True):
        try:
            pc_prefix = obj_type_map[object_type]
        except KeyError:
            return ''
        at=self.get_attr_prefix()

        if(check_if_exists):
            return {'preCondition':{at+'onFail':'MARK_RAN','not': { pc_prefix+'Exists':{at+pc_prefix+'Name':obj_name}}}}
        else:
            return {'preCondition':{at+'onFail':'MARK_RAN', pc_prefix+'Exists':{at+pc_prefix+'Name':obj_name}}}


    def add_to_changelog(self,event_type,author,id,schema,obj_name,obj_type,statement=None,filename=None,delimiter=';',precondition=''):
        chset = self.generate_changeset(event_type,author,id,schema,obj_name,obj_type,statement,filename,delimiter,precondition)
        self.add_step_to_changelog(chset)

