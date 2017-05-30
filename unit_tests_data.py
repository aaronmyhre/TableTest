import importlib
import pandas as pd
import json
import pprint
import sqlalchemy

# Schema
# goodPerson
# - id unique
# - person_type, two values "employee" & "customers"
# - is_active, Y or N
# - location_id. fk from local

# badPerson
# - id, not unique
# - person_type, three values "employee" & "customers" & "error"
# - is_active, Y, N, and null
# - location. fk from local, has non-valid keys

# location
# - location_id
# - location_name

# Create engine
engineConnectiontext = 'mysql://%s:%s@%s/%s' % ('tester', 'password', 'localhost', 'test')
engine = sqlalchemy.create_engine(engineConnectiontext, echo=False)

# drop tables
try:
    pd.read_sql_query("drop table goodPerson;", engine)
except:
    pass
try:
    pd.read_sql_query("drop table badPerson;", engine)
except:
    pass
try:
    pd.read_sql_query("drop table location;", engine)
except:
    pass

# create tables
engine.execute("create table location ( " +
    "id text, "+
    "location_name text "+
    ");"
    )

engine.execute("create table goodPerson ( "+
    "id text, "+
    "person_type text, "+
    "is_active text, "+
    "location_id text "+
    ");"
    )

engine.execute("create table badPerson ( " +
    "id text, "+
    "person_type text, "+
    "is_active text, "+
    "location_id text "+
    ");"
    )

# insert goodPerson
engine.execute("insert into goodPerson values (1,'employee','Y',100);")
engine.execute("insert into goodPerson values (2,'employee','N',101);")
engine.execute("insert into goodPerson values (3,'customer','Y',101);")
engine.execute("insert into goodPerson values (4,'customer','N',100);")

# insert badPerson
engine.execute("insert into badPerson values (1,'employee','Y',200);")
engine.execute("insert into badPerson values (1,'employee','N',101);")
engine.execute("insert into badPerson values (3,'customer',null,101);")
engine.execute("insert into badPerson values (3,'error','N',100);")

# insert badPerson
engine.execute("insert into location values (100,'South');")
engine.execute("insert into location values (101,'North');")
