import pandas as pd

debug = True

class Test(object):
    def __init__(self):
        pass

    def executeQuery(self, queryString):
        # Print parameters
        if debug == True:
            attrs = vars(self)
            attrs['test'] = type(self).__name__
            for attr in attrs:
                print("".join([attr,": ", str(attrs[attr])]))

        # Run query
        error = ''
        try:
            # execute query
            f = pd.read_sql_query(queryString, self.engine)

            # print results
            if debug == True:
                print("query results: " )
                print(f)

            # test
            if self.testCriteria == 'empty':
                if f.empty:
                    return (True, error)
                else:
                    return (False, self.error_reason(f))
            elif self.testCriteria == 'not empty':
                if not f.empty:
                    return (True, error)
                else:
                    return (False, self.error_reason(f))
            else:
                return (True, error)

        except Exception as e:
            return (False, e)

class DoesExist(Test):
    def __init__ (self,engine,table,doesExist):
        self.engine = engine
        self.table = table
        self.doesExist = doesExist
        self.testCriteria = 'valid'

    def construct_query(self):
        queryString = "SELECT 1 FROM %s LIMIT 1;" % (self.table)
        return queryString

    def error_reason(self, f):
        return "Table does not exist"

    def run(self):
        queryString = self.construct_query()
        return self.executeQuery(queryString)

class IsUnique(Test):
    def __init__(self,engine,table,field):
        self.engine = engine
        self.table = table
        self.field = field
        self.testCriteria = 'empty'

    def construct_query(self):
        queryString = ("SELECT %s duplicate_id, count(*) count_of_duplicates "+
                            "FROM %s "+
                            "group by %s "+
                            "having count(*) >= 2;") % (self.field, self.table, self.field)
        return queryString

    def error_reason(self, f):
        return "duplicate ids: " + str(list(f['duplicate_id']))

    def run(self):
        queryString = self.construct_query()
        return self.executeQuery(queryString)

class IsValidForeignKey(Test):
    def __init__(self,engine,table, parameters):
        self.engine = engine
        self.forignTable = table
        self.fk = parameters['fk']
        self.primaryTable = parameters['primaryTable']
        self.pk = parameters['pk']
        self.testCriteria = 'empty'

    def construct_query(self):
        queryString = ("SELECT distinctFT.distinctFK invalid_foreign_keys " +
                      "FROM " +
                      "(SELECT distinct(%s) distinctFK " +
                      "from %s) distinctFT " +
                      "left join %s pt " +
                      "on distinctFT.distinctFK = pt.%s " +
                      "where pt.%s is null " +
                      "group by distinctFT.distinctFK") % (self.fk, self.forignTable, self.primaryTable, self.pk, self.pk)
        return queryString

    def error_reason(self, f):
        return "Invalid Foreign Keys: " + str(list(f['invalid_foreign_keys']))

    def run(self):
        queryString = self.construct_query()
        return self.executeQuery(queryString)

class IsNotNull(Test):
    def __init__(self,engine,table,field):
        self.engine = engine
        self.table = table
        self.field = field
        self.testCriteria = 'empty'

    def construct_query(self):
        queryString = ("SELECT %s " +
                       "FROM %s " +
                       'WHERE %s is Null') % (self.field, self.table, self.field)
        return queryString

    def error_reason(self, f):
        return "field contained Null values"

    def run(self):
        queryString = self.construct_query()
        return self.executeQuery(queryString)

class IsEmpty(Test):
    def __init__(self,engine,table,field):
        self.engine = engine
        self.table = table
        self.field = field
        self.testCriteria = 'empty'

    def construct_query(self):
        queryString = ("SELECT count(*) " +
                       "FROM %s ") % (self.table)
        return queryString

    def error_reason(self, f):
        return "Table is not empty"

    def run(self):
        queryString = self.construct_query()
        return self.executeQuery(queryString)
