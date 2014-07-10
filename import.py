from sqlalchemy import Table, Column, Integer, String, Boolean, MetaData, ForeignKey
from sqlalchemy.exc import DataError, IntegrityError, ProgrammingError
import re
import yaml, collections
import types

def fetch(obj, key):
    return obj['key']
    
def useOrdered():
    _mapping_tag = yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG

    def dict_representer(dumper, data):
        return dumper.represent_mapping(_mapping_tag, data.iteritems())

    def dict_constructor(loader, node):
        return collections.OrderedDict(loader.construct_pairs(node))

    yaml.add_representer(collections.OrderedDict, dict_representer)
    yaml.add_constructor(_mapping_tag, dict_constructor)
    
class BaseColumn(object):
    def __init__(self,sql, source=None, necessary=None):
        self.sql = sql
        if source:
            self.source = source
            if source[0] == "$":
                self.value = eval(source[1:])
                def getData(self, obj):
                    return self.value
                self.getData = types.MethodType(getData,self,BaseColumn)
        else:
            self.source = sql.name
        self.necessary = necessary
        
    def getData(self, obj):
        try:
            return obj[self.source]
        except KeyError as ke:
            if self.necessary:
                print ke
                raise ke
            else:
                print ke
                return None

class LinkingColumn(BaseColumn):
    def __init__(self,sql, source=None, necessary=True, regex=None):
        BaseColumn.__init__(self,sql, source=source, necessary=necessary)
        if regex:
            self.regex = re.compile(regex)
        else:
            self.regex = None
            
    def getValues(self, obj):
        vals = set(BaseColumn.getData(self, obj))
        if self.regex:
            vals = filter(self.regex.match, vals)
        return vals   

class TableMapping():
    def __init__(self,name,cols,source=None, refresh=True):
        self.name = name
        self.cols = cols
        self.table = None
        self.filter = {}
        self.refresh = refresh
        if source is None:
            self.source = self.name
        else:
            self.source = source
    
    def makeTable(self, metadata):
        cols = self._getCols()
        self.table = Table(*[self.name, metadata]+self._getCols())
        return self.table
        
    def _getCols(self):
        return [col.sql for col in self.cols]
        
    def _getRow(self,obj):
        try:
            return { col.sql.name:col.getData(obj) for col in self.cols}
        except:
            pass#d = {}
            #for col in self.cols:
            #    try:
            #        col.
        
    def getValues(self, item):
        d = self._getRow(item)
        if d is None:
            return []
        return [d]

class MultiMapping(TableMapping):
    def __init__(self, name, cols, linking, source=None, refresh=False):
        self.linker = linking
        TableMapping.__init__(self, name, cols, source=source, refresh=refresh)
    
    def _getCols(self):
        return [self.linker.sql]+TableMapping._getCols(self)
        
    def _getRows(self, obj):
        vals = self.linker.getValues(obj)
        rows = [ ]
        for val in vals:
            d = TableMapping._getRow(self,obj)
            if d is None:
                return []
            d[self.linker.sql.name] = val
            rows.append(d)
        return rows
        
    def getValues(self,item):
        return self._getRows(item)


def attempt_eval(to_eval):
    try:
        eval(to_eval)
    except:
        return eval

class SchemaManager(object):
    def __init__(self, mappings=[], db="default"):
        self.metadata = MetaData()
        self.mappings = []
        self.db = db
        for m in mappings:
            self.addMapping(m)
        
    def addMapping(self,mapping):
        self.mappings.append(mapping)
        mapping.makeTable(self.metadata)
    
    def dropTables(self, sqla_engine):
        for mapping in reversed(self.mappings):
            if mapping.refresh:
                mapping.table.drop(sqla_engine,checkfirst=True)
        
    def makeTables(self, sqla_engine):
        for mapping in self.mappings:
            if mapping.refresh:
                mapping.table.create(sqla_engine,checkfirst=True)
        
    def wipeTables(self, sqla_engine):
        self.dropTables(sqla_engine)
        self.makeTables(sqla_engine)
        
    def import_all(self, sqla_engine, mongo_conn):
        db = mongo_conn[self.db]
        print "Making all tables."
        for schema in self.mappings:
            if not schema.refresh:
                print "Skipping table for:%s  > Told not to refresh." % schema.name
                continue
            ins = schema.table.insert()
            print "Making table for " + schema.name
            count = 0 
            row_num = 0
            error_count = 0 
            for item in db[schema.source].find(schema.filter).limit(10000):
                row_num += 1
                for row in schema.getValues(item):
                    try:
                        sqla_engine.execute(ins.values(**row))
                        count +=1
                    except DataError:
                        print "Could not import for %s :" % item['_id'], "data_error" 
                        error_count += 1
                    except IntegrityError as i:
                        print "Could not import for %s :" % item['_id'], "integrity_error"  
                        print i
                        error_count += 1
                    except ProgrammingError as p:
                        print "Could not import for %s :" % item['_id'], "programming_error"  
                        print p
                        error_count += 1
                    except Error as e:
                        print "Other unknown error.", e
                        error_count += 1
            print "  Loaded %s items" % (count)
            print "  Could not load %s items" % error_count

    @staticmethod
    def loadColumn(yaml_segment, clazz=BaseColumn):
            #try:
            sql = yaml_segment['sql']
            mongo = yaml_segment['mongo']
            
            if sql.has_key('name'):
                name = sql['name']
                del sql['name']
            else:
                name = mongo['source']
            
            type = eval(yaml_segment['sql']['type'])
            del yaml_segment['sql']['type']
            
            if sql.has_key('extra'):
                extra = [eval(value) for value in sql['extra']]
                del sql['extra']
            else:
                extra = []
            
            values = [name,type] + extra
            
            return clazz(Column(*values, **sql), **mongo)
                    
    @staticmethod
    def loadFromYaml(yaml_file_name, ignore_refresh=False):
        with open(yaml_file_name, 'r') as f:
            doc = yaml.load(f, )
            db = doc['db']
            tables = doc['tables']
            mappings = []
            for name,table in tables.iteritems():
                src_table = table['source']
                cols = [SchemaManager.loadColumn(content) for content in table['columns']]
                if table.has_key('refresh') and not ignore_refresh:
                    refresh = table['refresh']
                else:
                    refresh = True
                if table.has_key('linking'):
                    linker = SchemaManager.loadColumn(table['linking'], clazz=LinkingColumn)
                    schema = MultiMapping(name,cols,linker,source=src_table, refresh=refresh)
                else:
                    schema = TableMapping(name,cols,source=src_table, refresh=refresh)
                if table.has_key('filter'):
                    schema.filter = table['filter']
                mappings.append(schema)
            return SchemaManager(mappings = mappings, db = db)
                
                
from pymongo import MongoClient
from sqlalchemy import create_engine

class DBConnections():
    def __init__(self,host, port, db_name, sql_uri):
        self.conn = MongoClient(host=host, port=port)
        self.engine = create_engine(sql_uri)
        
def runImport(connections, scheme_manager):
    scheme_manager.wipeTables(connections.engine)
    scheme_manager.import_all(connections.engine, connections.conn)
    
    
if __name__ == "__main__":
    from pymongo.errors import ConnectionFailure
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-host", help="the host mongo is on", type=str, default="localhost")
    parser.add_argument("--mongo-port", help="the port mongo is on", type=str, default=27017)
    parser.add_argument("--mongo-db", help="the mongo db name", type=str, default="default")
    parser.add_argument("--ps-uri", help="the uri for the postgresql table", type=str, default="localhost")
    parser.add_argument("--schema", help="the location of the python file to use", type=str, default="scheme.yaml")
    args = parser.parse_args()
    try:
        dbconns = DBConnections(args.mongo_host, args.mongo_port, args.mongo_db, 'postgresql:'+args.ps_uri)
        useOrdered()
        sm = SchemaManager.loadFromYaml(args.schema,True)
        print sm.mappings
        runImport(dbconns, sm)
    except ConnectionFailure as e:
        print "Internal Error", e
    
    
