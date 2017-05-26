import pandas as pd

debug = True

class Test(object):
    def __init__(self):
        pass

class Unique(object,):
    def __init__(self,engine,table,field):
        self.engine = engine
        self.table = table
        self.field = field

        if debug == True:
            print("engine: %s" % (self.engine))
            print("table: %s" % (self.table))
            print("field: %s" % (self.field))

    def run(self):

        # Construct Query
        queryString = ("SELECT %s, count(*) "+
                      "FROM %s "+
                      "group by %s "+
                      "having count(*) >= 2;") % (self.field, self.table, self.field)
        if debug == True:
            print("queryString: %s" % (queryString))

        # Run query
        try:
            f = pd.read_sql_query(queryString, self.engine)
            if f.empty:
                return True
            else:
                return False
        except:
            return False

class Exists(object):
    def __init__ (self,engine,table,doesExist):
        self.engine = engine
        self.table = table
        self.doesExist = doesExist

        if debug == True:
            print("engine: %s" % (self.engine))
            print("table: %s" % (self.table))
            print("doesExist: %s" % (self.doesExist))

    def run(self):

        queryString = "SELECT 1 FROM %s LIMIT 1;" % (self.table)
        if debug == True:
            print("queryString: %s" % (queryString))

        try:
            f = pd.read_sql_query(queryString, self.engine)
            result = True
        except:
            result = False

        return result
