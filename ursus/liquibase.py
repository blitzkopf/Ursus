import re
#import xmltodict
import yaml
import pprint
#import  xml.etree.ElementTree as ET
from lxml import etree as ET

obj_type_map = {'INDEX':'index','TABLE':'table', 'PACKAGE':'package', 'PACKAGE BODY':'packageBody' }

obj_type_LB_map = {'INDEX':'Index','TABLE':'Table', 'PACKAGE':'Package', 'PACKAGE BODY':'PackageBody' }

class Changelog:
    def __init__(self,filename):
        self.filename = filename
        self.reset_file = False
        self.changesets=[]
        self.format = 'yaml'
        self.root=ET.Element('databaseChangeLog',attrib={
                 'xmlns':"http://www.liquibase.org/xml/ns/dbchangelog"},
                 nsmap= {'xsi': "http://www.w3.org/2001/XMLSchema-instance" }
                )
        self.root.attrib['{http://www.w3.org/2001/XMLSchema-instance}schemaLocation']='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.8.xsd'

 
    def read_changelog(self):
        if(self.format=='xml'):
            try:
                tree = ET.parse(self.filename)
                self.root = tree.getroot()
                self.changesets = self.root.findall('changeSet')
            except IOError:
                return
        elif(self.format=='yaml'):
            try:
                with open(self.filename) as f:
                    chl_dict=yaml.safe_load(f)
            except IOError:
                return

            pprint.pprint(chl_dict)
            self.changesets = chl_dict['databaseChangeLog']

    def write_changelog(self):
        if(self.format=='xml'):
            tree = ET.ElementTree(self.root)
            #ET.register_namespace('', 'http://www.liquibase.org/xml/ns/dbchangelog')

            tree.write(self.filename,pretty_print=True)
            
        elif(self.format=='yaml'):
            with open(self.filename,'w') as f:
                f.write(yaml.safe_dump({'databaseChangeLog':self.changesets}) )

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
        changeset = {'id':id,'author':author}
        changes = []
        if(precondition):
            changeset['preConditions']=precondition
            
            #print("adding precond"+ET.tostring(precondition))
            ##changeset['changeSet']['preCondition']=precondition['preCondition'] 
        if(statement):
            changes.append({'sql':{
                        'endDelimiter':delimiter,
                        'splitStatements':"true" ,
                        'stripComments':"false",'sql':statement}}
            )
            
        elif (filename):
            if(obj_type == 'TABLE'): # 
                changeset['runOnChange'] = "false"
                changeset['validCheckSum']=None # This is not really working as supposed
            else:
                changeset['runOnChange'] = "true"
            changes.append( {'sqlFile': {
                        'endDelimiter':delimiter,
                        'splitStatements':"true" ,
                        'stripComments':"false",
                        'relativeToChangelogFile':"true",
                        'path':filename
            }})
        changeset['changes'] = changes
        #self.root.append(changeset)
        return {'changeSet':changeset }
    
    def get_precond(self,obj_name,object_type,check_if_exists=True):
        try:
            pc_prefix = obj_type_map[object_type]
        except KeyError:
            return ''
        cond = {pc_prefix+'Exists':{pc_prefix+'Name':obj_name}}
        if(check_if_exists):  
            cond =  {'not':[cond]}
        return [{'onFail':'MARK_RAN'},cond]

    def add_to_changelog(self,event_type,author,id,schema,obj_name,obj_type,statement=None,filename=None,delimiter=';',precondition=''):
        chset = self.generate_changeset(event_type,author,id,schema,obj_name,obj_type,statement,filename,delimiter,precondition)
        self.add_step_to_changelog(chset)

