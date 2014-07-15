from sqlalchemy import Table, Column, Integer, String, Boolean, MetaData, ForeignKey, ForeignKeyConstraint, PrimaryKeyConstraint
from sqlalchemy.exc import DataError, IntegrityError, ProgrammingError
from sqlalchemy.dialects.postgresql import BYTEA
import re
import yaml, collections
import types

from sqlalchemy.schema import CreateTable



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
    def __init__(self,sql=None, source=None, necessary=None):
        if sql == None:
            sql = source
        if source == None:
            source = sql
        self.source = source
        self.sql = sql
        if source[0] == "$":
            self.value = eval(source[1:])
            def getData(self, obj):
                return self.value
            self.getData = types.MethodType(getData,self,BaseColumn)
        self.necessary = necessary
        
    def getData(self, obj):
        try:
            return obj[self.source]
        except KeyError as ke:
            if self.necessary:
                print ke
                raise ke
            else:
                print "   For: '%s' could not read '%s'" % (obj['_id'], ke)
                return None


class LinkingColumn(BaseColumn):
    def __init__(self,source=None, necessary=True, regex=None):
        BaseColumn.__init__(self,source=source, necessary=necessary)
        if regex:
            self.regex = re.compile(regex)
        else:
            self.regex = None
            
    def getValues(self, obj):
        values = set(BaseColumn.getData(self, obj))
        if self.regex:
            values = filter(self.regex.match, values)
        return values


class TableSource():
    def __init__(self, name, cols):
        self.name = name
        self.cols = cols
        self.filter = {}

    def _getRow(self, obj):
        try:
            return { col.sql:col.getData(obj) for col in self.cols}
        except:
            print "Could not get values"
            raise
            pass
        
    def getValues(self, obj):
        r = self._getRow(obj)
        if r:
            return [r]
        return []


class LinkingSource():
    def __init__(self, name, cols, linker):
        self.name = name
        self.cols = cols
        self.linker = linker
        self.filter = {}
        
    def getValues(self,item):
        values = self.linker.getValues(item)
        rows = [ ]
        for value in values:
            d = TableMapping._getRow(self,item)
            if d is None:
                return []
            d[self.linker.sql.name] = value
            rows.append(d)
        return rows
            
class TableMapping():
    def __init__(self, dest, sources):
        self.dest = dest
        self.sources = sources

    @property
    def name(self):
        return self.dest.name

    def make_table(self, metadata):
        self.table = self.dest.make_table(metadata)
                     
class TableDest():
    def __init__(self, name, cols,extra=[]):
        self.name = name
        self.cols = cols
        self.extra = extra
        
    def make_table(self, metadata):
        args = [self.name,metadata] + self.cols + self.extra
        self.table =  Table(*args)
        return self.table

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
        print mapping, mapping.name
        mapping.make_table(self.metadata)
    
    def dropTables(self, sqla_engine):
        for mapping in reversed(self.mappings):
            mapping.table.drop(sqla_engine,checkfirst=True)
        
    def make_tables(self, sqla_engine):
        for mapping in self.mappings:
            print CreateTable(mapping.table).compile(sqla_engine)
            mapping.table.create(sqla_engine,checkfirst=True)
        
    def wipeTables(self, sqla_engine):
        self.dropTables(sqla_engine)
        self.make_tables(sqla_engine)
        
    def import_all(self, sql_engine, mongo_conn, limit=1000):
        print "Making all tables."
        for mapping in self.mappings:
            sql = mapping.dest
            print "Making the %s postgres table" % (sql.name)
            for source in mapping.sources:
                print " Loading data from the %s mongo collection" % (source.name)
                self.import_table(sql_engine, mongo_conn, sql, source, limit=limit)

    def import_table(self, sql_engine, mongo_conn, sql, source, limit=1000):
            db = mongo_conn[self.db]
            ins = sql.table.insert()
            count = 0
            row_num = 0
            error_count = 0
            for item in db[source.name].find(source.filter).limit(limit):
                row_num+= 1
                for row in source.getValues(item):
                    try:
                        sql_engine.execute(ins.values(**row))
                        count +=1
                    except DataError:
                        print "   Could not import for %s :" % item['_id'], "data_error"
                        error_count += 1
                    except IntegrityError as i:
                        print "   Could not import for %s :" % item['_id'], "integrity_error"
                        #print i
                        error_count += 1
                    except ProgrammingError as p:
                        print "   Could not import for %s :" % item['_id'], "programming_error"
                        #print p
                        error_count += 1
                    except Exception as e:
                        print "   Other unknown error.", e
                        error_count += 1
            print "  Loaded %s items" % (count)
            print "  Could not load %s items" % error_count
            print "  Processed %s items" % (row_num)

class Importer():
    @staticmethod
    def sql_col(sql):
        name = sql['name']
        del sql['name']
        type = eval(sql['type'])
        del sql['type']
        
        if sql.has_key('extra'):
            extra = [eval(value) for value in sql['extra']]
            del sql['extra']
        else:
            extra = []
        
        values = [name,type] + extra
        
        return Column(*values)
        
                
    @staticmethod
    def mongo_col(mongo, clazz=BaseColumn):
        return clazz(**mongo)
    
    @staticmethod
    def loadFromYaml(yaml_file_name, ignore_refresh=False):
        with open(yaml_file_name, 'r') as f:
            doc = yaml.load(f, )
            db = doc['db']
            tables = doc['tables']
            mappings = []
            for name,table in tables.iteritems():
                sql = table['sql']
                if sql.has_key('extra'):
                    extra = [eval(ext) for ext in sql['extra']]
                else:
                    extra = []
                dest = TableDest(sql['name'],[Importer.sql_col(col) for col in sql['columns']], extra=extra)


                mongos = table['mongo']
                sources = []
                for item in mongos:
                    name = item['name']
                    cols = [Importer.mongo_col(col) for col in item['columns']]
                    if item.has_key("linker"):
                        mongo_table = LinkingSource(name, cols, Importer.mongo_col(item['linker']))
                    else:
                        mongo_table = TableSource(name, cols)
                    if item.has_key("filter"):
                        mongo_table.filter = item['filter']
                    sources.append(mongo_table)
                
                mappings.append(TableMapping(dest, sources))
            return SchemaManager(mappings = mappings, db = db)
                
from pymongo import MongoClient
from sqlalchemy import create_engine

class DBConnections():
    def __init__(self,host, port, db_name, sql_uri):
        self.conn = MongoClient(host=host, port=port)
        self.engine = create_engine(sql_uri)


def runImport(connections, scheme_manager, tables = [], limit=1000):
    if tables:
        mappings = [mapping for mapping in scheme_manager.mappings if mapping.name in tables]
        scheme_manager.mappings = mappings
        print tables, mappings
    scheme_manager.wipeTables(connections.engine)
    scheme_manager.import_all(connections.engine, connections.conn, limit = limit)
    
    
if __name__ == "__main__":
    from pymongo.errors import ConnectionFailure
    
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongo-host", help="the host mongo is on", type=str, default="localhost")
    parser.add_argument("--mongo-port", help="the port mongo is on", type=str, default=27017)
    parser.add_argument("--mongo-db", help="the mongo db name", type=str, default="default")
    parser.add_argument("--ps-uri", help="the uri for the postgresql table", type=str, default="localhost")
    parser.add_argument("--schema", help="the location of the python file to use", type=str, default="scheme.yaml")
    parser.add_argument("--tables", help="which tables to process", type=str, default="")
    parser.add_argument("--limit", help="what to limit each table to", type=int, default=20000)
    args = parser.parse_args()
    try:
        dbconns = DBConnections(args.mongo_host, args.mongo_port, args.mongo_db, 'postgresql:'+args.ps_uri)
        useOrdered()
        sm = Importer.loadFromYaml(args.schema,False)
        if args.tables:
            tables = args.tables.split(",")
        else:
            tables =[]
        runImport(dbconns, sm, tables=tables, limit=args.limit)
    except ConnectionFailure as e:
        print "Internal Error", e
