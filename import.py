from sqlalchemy import Table, Column, Integer, String, Boolean, MetaData, ForeignKey

class TableMapping():
    def __init__(self,name,cols,source=None):
        self.name = name
        self.cols = cols
        if source is None:
            self.source = self.name
        else:
            self.source = source
    
    def makeTable(self, metadata):
        cols = self._getCols()
        return Table(*[self.name, self.metadata]+self._getCols())
        
    def _getCols(self):
        return [col for col in self.cols.values()]
        
    def _getRow(self,obj):
        return { col.name:obj[key] for key,col in self.cols.iteritems()}
        
    def getValuess(self, item):
        return [self._getRow(item)]

class MultiMapping(TableMapping):
    def __init__(self, name, cols, multi, multi_source, source=None):
        self.multi = multi
        self.multi_source = multi_source
        TableMapping.__init__(self, name, cols, source=source)
    
    def _getCols(self):
        return [self.multi]+TableMapping._getCols(self)
        
    def _getRows(self, obj):
        vals = obj[self.multi_source]
        rows = [ ]
        for val in vals:
            d = TableMapping._getRow(self,obj)
            d[self.multi.name] = val
            rows.append(d)
        return rows
        
    def getValuess(self,item):
        return self._getRows(item)

class SchemaManager(object):
    def __init__(self, mappings=[], db="default"):
        self.metadata = Metadata()
        self.mappings = []
        self.db = db
        
    def addMapping(self,mapping):
        self.mappings.append(mapping)
        
    def createTables(self, sqla_engine):
        for mapping in self.mappings:
            mapping.makeTable(self,self.metadata)
        self.metadata.create_all(sqla_engine)
        
    def import_all(self, sqla_engine, mongo_conn):
        db = mongo_conn[self.db]
        for schema in self.mappings:
            print schema.name
            ins = schema.table.insert()
            for item in db[schema.source].find().limit(100):
                for row in schema.getValuess(item):
                    engine.execute(ins.values(**row))

    @staticmethod
    def loadColumn(source, yaml_segment):
        if yaml_segment.has_key('name'):
            name = yaml_segment['name']
            del yaml_segment['name']
        else:
            name = source
        type = eval(yaml_segment['type'])
        del yaml_segment['type']
        return Column(name, type, **yaml_segment)
                    
    @staticmethod
    def loadFromYaml(yaml_file_name):
        import yaml
        with open(yaml_file_name, 'r') as f:
            doc = yaml.load(f)
            db = doc['db']
            tables = doc['tables']
            mappings = []
            for name,table in tables.iteritems():
                src_table = table['source']
                cols = [SchemaManager.loadColumn(source, content) for source,content in table['columns'].iteritems()]
                print src_table, name, [col.name for col in cols]
                if table.has_key('linking'):
                    key = table['linking'].keys()[0]
                    value = table['linking'][key]
                    col = loadColumn(key,value)
                    schema = MultiMapping(name,cols,key,col,source=src_table)
                else
                    schema = TableMapping(name,cols,source=src_table)
                mapping.append(schema)
        return SchemaManager(mappings, db = db)
                
                
from pymongo import MongoClient
from sqlalchemy import create_engine

class DBConnections():
    def __init__(self,host, port, db_name, sql_uri):
        self.conn = MongoClient(host=host, port=port)
        self.engine = create_engine(sql_uri)
        
def runImport(connections, scheme_manager):
    scheme_manager.createTables(connections.engine)
    scheme_manager.import_all(connections.engine, connections.conn)
    
from pymongo.errors import ConnectionFailure
    
if __name__ == "__main__":
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
        SchemaManager.loadFromYaml(args.schema)
    except ConnectionFailure as e:
        print "Internal Error", e
    
    
