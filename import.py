from sqlalchemy import Table, Column, Integer, String, DateTime, MetaData, ForeignKey, ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.exc import DataError, IntegrityError, ProgrammingError
from sqlalchemy import select, insert
from sqlalchemy.dialects.postgresql import BYTEA, BOOLEAN
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
    def __init__(self,sql=None, source=None, necessary=None, converter = None):
        if sql == None:
            sql = source
        if source == None:
            source = sql
        self.source = source
        self.sql = sql
        if source[0] == "$":
            self.value = eval(source[1:])
            self.getData = self.get_value
        elif converter:
            self.converter = converter
            self.getData = self.get_converted
        else:
            self.getData = self.get_data
        self.necessary = necessary

    def getData(self):
        pass # assigned in constructor

    def get_data(self, obj):
        try:
            return obj[self.source]
        except KeyError as ke:
            if self.necessary:
                print ke
                raise ke
            else:
                print "   For: '%s' could not read '%s'" % (obj['_id'], ke)

    def get_value(self, obj):
        return self.value

    def get_converted(self, obj):
        return self.converter.lookup(self.get_data(obj))

class LinkingColumn(BaseColumn):
    def __init__(self,sql=None, source=None, necessary=True, regex=None, converter = None):
        BaseColumn.__init__(self, sql=sql, source=source, necessary=necessary)
        if regex:
            self.regex = re.compile(regex)
            self.get_values = self.get_filtered_values
        else:
            self.get_values = self.get_base_values
        if converter:
            self.converter = converter
            self.getValues = self.get_converted
        else:
            self.getValues = self.get_values

    def getValues(self,obj):
        pass # assigned in constructor

    def get_values(self,obj):
        pass # assigned in constructor

    def get_filtered(self, obj):
        return filter(self.regex.match,self.get_base_values(obj))

    def get_base_values(self, obj):
        return set(BaseColumn.get_data(self, obj))

    def get_converted(self, obj):
        return [self.converter.lookup(value) for value in self.get_values(obj)]


class TableSource(object):
    def __init__(self, name, cols):
        self.name = name
        self.cols = cols
        self.filter = {}

    def _getRow(self, obj):
        try:
            return { col.sql:col.getData(obj) for col in self.cols}
        except:
            print "   ! Could not get values !"
            raise
            pass

    @property
    def restricter(self):
        return {col.source:1 for col in self.cols}
        
    def getValues(self, obj):
        r = self._getRow(obj)
        if r:
            return [r]
        return []


class LinkingSource(TableSource):
    def __init__(self, name, cols, linker):
        self.name = name
        self.cols = cols
        self.linker = linker
        self.filter = {}

    @property
    def restricter(self):
        return {col.source:1 for col in self.cols+[self.linker]}

    def getValues(self,item):
        values = self.linker.getValues(item)
        rows = [ ]
        for value in values:
            d = TableSource._getRow(self,item)
            if d is None:
                return []
            d[self.linker.sql] = value
            rows.append(d)
        return rows
        
class Converter():
    def __init__(self, name, type):
        self.name = name
        self.type = type

    def make_table(self, metadata):
        self.table = Table(self.name, metadata, Column('id',Integer),Column('val',self.type),PrimaryKeyConstraint('id'), UniqueConstraint('val'))
    def bind(self, engine):
        self.engine = engine

    def lookup(self, value):
        try:
            return self.engine.execute(select([self.table.c.id]).where(self.table.c.val==value)).first()['id']
        except Exception as e:
            if str(e) == "'NoneType' object has no attribute '__getitem__'":
                return self.engine.execute(self.table.insert().values(val=value)).inserted_primary_key[0]
            else:
                raise
            
class TableMapping():
    def __init__(self, dest, sources):
        self.dest = dest
        self.sources = sources
        self.table = None

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


def eval_dict(to_eval):
    if type(to_eval) == type({'a':None}):
        for key in to_eval:
            to_eval[key] = eval_dict(to_eval[key])
        return to_eval
    if type(to_eval) == type("asdf"):
        return attempt_eval(to_eval)
    else:
        return to_eval


def attempt_eval(to_eval):
    try:
        eval(to_eval)
    except:
        return to_eval


class SchemaManager(object):
    def __init__(self, mappings=[], db="default", converters = {}):
        self.metadata = MetaData()
        self.mappings = []
        self.db = db
        for m in mappings:
            self.addMapping(m)
        for k,v in converters.iteritems():
            v.make_table(self.metadata)
        self.converters = [v for k,v in converters.iteritems()]
        
    def addMapping(self,mapping):
        self.mappings.append(mapping)
        mapping.make_table(self.metadata)

    def init_converters(self, sqla_engine):
        for converter in self.converters:
            converter.bind(sqla_engine)
            converter.table.create(sqla_engine,checkfirst=True)
    
    def dropTables(self, sqla_engine):
        for mapping in reversed(self.mappings):
            mapping.table.drop(sqla_engine,checkfirst=True)
        
    def make_tables(self, sqla_engine):
        for mapping in self.mappings:
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

            for item in db[source.name].find(source.filter, source.restricter).limit(limit):
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
                        #raise
                        error_count += 1
                    except ProgrammingError as p:
                        print "   Could not import for %s :" % item['_id'], "programming_error"
                        #print p
                        error_count += 1
                    except Exception as e:
                        print "   Other unknown error.", e
                        error_count += 1
                    if row_num % 200 == 0:
                        print  "   progress: %s items imported " % (row_num)
            print "  %s items added" % (count)
            print "  %s items NOT added" % error_count
            print "  %s rows process" % (row_num)


class Import():
    def __init__(self, yaml_file_name):
        self.converters = {}
        self.value = self.load_from_yaml(yaml_file_name)
    
    def sql_col(self,sql):
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
                
    def mongo_col(self,mongo, clazz=BaseColumn):
        mongo = {k:v for k,v in mongo.iteritems()}
        if 'converter' in mongo:
            print ">>>>>>>>>>>%s<<<<<<<<<<" % (mongo['converter'])
            mongo['converter'] = self.converters[mongo['converter']]
        return clazz(**mongo)
    
    def load_from_yaml(self,yaml_file_name):
        with open(yaml_file_name, 'r') as f:
            doc = yaml.load(f, )
            db = doc['db']
            tables = doc['tables']
            mappings = []
            if "converters" in doc:
                self.converters = { name: Converter(name, eval(type)) for name,type in doc['converters'].iteritems() }
            for name,table in tables.iteritems():
                sql = table['sql']
                if sql.has_key('extra'):
                    extra = [eval(ext) for ext in sql['extra']]
                else:
                    extra = []
                dest = TableDest(sql['name'],[self.sql_col(col) for col in sql['columns']], extra=extra)


                mongos = table['mongo']
                sources = []
                for item in mongos:
                    name = item['name']
                    cols = [self.mongo_col(col) for col in item['columns']]
                    if item.has_key("linking"):
                        mongo_table = LinkingSource(name, cols, self.mongo_col(item['linking'],clazz=LinkingColumn))
                    else:
                        mongo_table = TableSource(name, cols)
                    if item.has_key("filter"):
                        mongo_table.filter = item['filter']
                        for key in mongo_table.filter:
                            try: 
                                mongo_table.filter[key]['$not'] = re.compile(mongo_table.filter[key]['$not'])
                            except:
                                pass
                        mongo_table.filter = eval_dict(mongo_table.filter)
                    sources.append(mongo_table)
                
                mappings.append(TableMapping(dest, sources))
            return SchemaManager(converters = self.converters, mappings = mappings, db = db)
                
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
    scheme_manager.init_converters(connections.engine)
    scheme_manager.wipeTables(connections.engine)
    scheme_manager.import_all(connections.engine, connections.conn, limit = limit)

import sys

class Tee(object):
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)

def use_std_file(fout):
    f = open(fout, 'w')
    original = sys.stdout
    sys.stdout = Tee(sys.stdout, f)
    return f
    
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
    parser.add_argument("--output", help="where to dump the log", type=str, default="")
    args = parser.parse_args()
    try:
        if args.output:
            f = use_std_file(args.output)
        else:
            f = None
        dbconns = DBConnections(args.mongo_host, args.mongo_port, args.mongo_db, 'postgresql:'+args.ps_uri)
        useOrdered()
        sm = Import(args.schema).value
        if args.tables:
            tables = args.tables.split(",")
        else:
            tables =[]
        runImport(dbconns, sm, tables=tables, limit=args.limit)
        try:
            f.close()
        except:
            pass
    except ConnectionFailure as e:
        print "Internal Error", e
