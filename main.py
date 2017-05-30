import importlib
import pandas as pd
import json
import pprint
from sqlalchemy import create_engine

class TableTest(object):
    def __init__(self, tests, user, password, dbPath, database ):
        # tests
        with open(tests, 'r') as f:
            try:
                self.tests = json.load(f)
            except ValueError:
                raise Exception("not a valid json")


        # define connection string and engine
        engineConnectionString = 'mysql://%s:%s@%s/%s' % (user, password, dbPath, database)
        self.engine = create_engine(engineConnectionString, echo=False)

    def run(self):
        results = []
        for table in self.tests:
            tests = self.tests[table]
            for testName in tests:
                TestClass = getattr(importlib.import_module("tests"), testName)
                parameters = self.tests[table][testName]
                test = TestClass(self.engine, table, parameters)
                testResult = test.run()
                results.append((table, testName, parameters, testResult))

        for r in results:
            print(r)



if __name__ == "__main__":
    test = TableTest('unit_tests.json', 'tester', 'password', 'localhost', 'test')
    results = test.run()
