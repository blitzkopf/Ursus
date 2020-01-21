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
            chl_string = ''
            try:
                with open(self.filename) as f:
                    for line in f.readlines():
                        chl_string += line
            except IOError:
                return

            chl_dict = yaml.parse(chl_string)
    @classmethod
    def yamler(cls,element):
        subs =[]
        subs = [cls.yamler(el) for el in element ]
        #if element.tag == 'databaseChangeLog':
            
        return {element.tag:subs}

    def write_changelog(self):
        if(self.format=='xml'):
            tree = ET.ElementTree(self.root)
            #ET.register_namespace('', 'http://www.liquibase.org/xml/ns/dbchangelog')

            tree.write(self.filename,pretty_print=True)
            
        elif(self.format=='yaml'):
            #for elem in self.root.iter():
            ychangelog = self.yamler(self.root)
            #for elem in self.root:
            #    key,value = self.yamler(elem)
            #    print(elem)
            #    print(elem.attrib)
                
            #chl_string = xmltodict.unparse({'databaseChangeLog':self.changesets})
            #chl_dict = yaml.parse(ychangelog)
            with open(self.filename,'w') as f:
                f.write(yaml.safe_dump(ychangelog) )

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
        changeset = ET.Element('changeSet',{'id':id,'author':author})
        self.root.append(changeset)
        if(precondition):
            changeset.append(precondition)
            
            #print("adding precond"+ET.tostring(precondition))
            ##changeset['changeSet']['preCondition']=precondition['preCondition'] 
        if(statement):
            changeset.append(ET.Element('sql',{
                        'endDelimiter':delimiter,
                        'splitStatements':"true" ,
                        'stripComments':"false"},text=statement)
            )
        elif (filename):
            if(obj_type == 'TABLE'):
                run_on_change = "false"
            else:
                run_on_change = "true"
            changeset.attrib['runOnChange']=run_on_change
            changeset.append(ET.Element( 'sqlFile', {
                        'endDelimiter':delimiter,
                        'splitStatements':"true" ,
                        'stripComments':"false",
                        'relativeToChangelogFile':"true",
                        'path':filename
            }))
        return changeset
    
    def get_precond(self,obj_name,object_type,check_if_exists=True):
        try:
            pc_prefix = obj_type_map[object_type]
        except KeyError:
            return ''
        pc = ET.Element('preConditions',attrib={'onFail':'MARK_RAN'})
        if(check_if_exists):  
            ET.SubElement(ET.SubElement(pc,'not'),pc_prefix+'Exists',attrib={pc_prefix+'Name':obj_name})
        else:
            ET.SubElement(pc,pc_prefix+'Exists',attrib={pc_prefix+'Name':obj_name})
        return pc

    def add_to_changelog(self,event_type,author,id,schema,obj_name,obj_type,statement=None,filename=None,delimiter=';',precondition=''):
        chset = self.generate_changeset(event_type,author,id,schema,obj_name,obj_type,statement,filename,delimiter,precondition)
        self.add_step_to_changelog(chset)

