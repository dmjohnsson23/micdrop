import sys, os
# To allow importing local version of package
sys.path.insert(0, os.path.realpath(os.path.join(os.path.basename(__file__), '../')))
from micdrop import *
from micdrop.ext.csv import *
from micdrop.ext.sql_alchemy import *
from sqlalchemy import *
from datetime import date
import re
from functools import partial

# Create a database for a hypothetical example system
engine = create_engine('sqlite:///example1.db')
meta = MetaData()
t_users = Table('users', meta,
    Column('id', Integer, primary_key=True),
    Column('username', String(255)),
    Column('active', Boolean),
    Column('pass_hash', String(255)),
    Column('email', String(255)),
    Column('f_name', String(255)),
    Column('l_name', String(255)),
    Column('sex', String(1)),
    Column('dob', Date),
    Column('occupation', String(255)),
    Column('registration_date', Date),
    Column('_tmp_id', Integer), # Add a temporary column for storing the old system IDs
)
t_user_phones = Table('user_phones', meta, 
    Column('id', Integer, primary_key=True),
    Column('user_id', Integer, ForeignKey('users.id'), nullable=False),
    Column('type', String(255)),
    Column('display', String(255)),
    Column('search', String(255)),
)
t_clients = Table('clients', meta, 
    Column('id', Integer, primary_key=True),
    Column('name', String(255)),
    Column('website', String(255)),
    Column('registration_date', Date),
    Column('_tmp_id', Integer), # Add a temporary column for storing the old system IDs
)
t_client_addresses = Table('client_addresses', meta, 
    Column('id', Integer, primary_key=True),
    Column('client_id', Integer, ForeignKey('clients.id'), nullable=False),
    Column('line1', String(255)),
    Column('line2', String(255)),
    Column('city', String(255)),
    Column('state', String(255)),
    Column('zip', String(255)),
    Column('county', String(255)),
)
t_client_contacts = Table('client_contacts', meta, 
    Column('id', Integer, primary_key=True),
    Column('client_id', Integer, ForeignKey('clients.id'), nullable=False),
    Column('f_name', String(255)),
    Column('l_name', String(255)),
    Column('title', String(255)),
    Column('email', String(255)),
    Column('_tmp_id', Integer), # Add a temporary column for storing the old system IDs
)
t_client_contact_phones = Table('client_contact_phones', meta, 
    Column('id', Integer, primary_key=True),
    Column('client_contact_id', Integer, ForeignKey('client_contacts.id'), nullable=False),
    Column('type', String(255)),
    Column('display', String(255)),
    Column('search', String(255)),
)
meta.create_all(engine)

# For the first pass, we'll import legacy users into the new system
source = CSVSource(os.path.join(os.path.dirname(__file__), 'data/people-100.csv'))
sink = TableInsertSink(engine, t_users)

# Define mappings for common fields
source.take('Index') >> sink.put('_tmp_id')
source.take('First Name') >> sink.put('f_name')
source.take('Last Name') >> sink.put('l_name')
source.take('Email') >> sink.put('email')
source.take('Job Title') >> sink.put('occupation')
# Use strings in the pipeline as templates
source.take('User Id') >> "_legacy_user_{}" >> sink.put('username')
# Use dictionaries as lookup tables to convert values
source.take('Sex') >> {'Male':'m', 'Female':'f'} >> sink.put('sex')
# Predefined pipeline items for common tasks also exist
source.take('Date of birth') >> ParseDate() >> sink.put('dob')
# Use StaticSource or FactorySource to provide defaults for values not found in the source data
FactorySource(date.today) >> sink.put('registration_date')
StaticSource(False) >> sink.put('active')

# Actually execute the pipelines
process_all(sink)

# Create a reusable lookup table for mapping user IDs in other tables
user_lookup = PipelineSegment() >> LookupTable(engine, t_users, t_users.c._tmp_id, t_users.c.id)

# The new system uses a separate table for storing phone number data, so we make a second pass 
# over the file to extract that
source = CSVSource(os.path.join(os.path.dirname(__file__), 'data/people-100.csv'))
sink = TableInsertSink(engine, t_user_phones)

StaticSource('Other') >> sink.put('type')
# We can use our lookup table to map the user IDs correctly
source.take('Index') >> user_lookup >> sink.put('user_id')
# We can use `SkipIf` to skip rows that don't have a value in this column
source.take('Phone') >> (SkipIf.value == '') >> sink.put('display')
# Functions or other callables can be used in pipelines for complex transformations
source.take('Phone') >> partial(re.compile('[^0-9]').sub, '') >> sink.put('search')

# Execute the second pass over the file
process_all(sink)

# We can import from another file in much the same way
source = CSVSource(os.path.join(os.path.dirname(__file__), 'data/customers-100.csv'))
sink = TableInsertSink(engine, t_clients)

source.take('Index') >> sink.put('_tmp_id')
source.take('Company') >> sink.put('name')
source.take('Subscription Date') >> ParseDate() >> sink.put('registration_date')
source.take('Website') >> sink.put('website')

process_all(sink)
client_lookup = PipelineSegment() >> LookupTable(engine, t_clients, t_clients.c._tmp_id, t_clients.c.id)

# A second pass with that file
source = CSVSource(os.path.join(os.path.dirname(__file__), 'data/customers-100.csv'))
sink = TableInsertSink(engine, t_client_contacts)

source.take('Index') >> sink.put('_tmp_id')
source.take('Index') >> client_lookup >> sink.put('client_id')
source.take('First Name') >> sink.put('f_name')
source.take('Last Name') >> sink.put('l_name')
source.take('Email') >> sink.put('email')

process_all(sink)
client_contact_lookup = PipelineSegment() >> LookupTable(engine, t_client_contacts, t_client_contacts.c._tmp_id, t_client_contacts.c.id)

# The `RepeaterSource` wrapper can be used to normalize data that appears in repeat columns
source = RepeaterSource(CSVSource(os.path.join(os.path.dirname(__file__), 'data/customers-100.csv')))
sink = TableInsertSink(engine, t_client_contact_phones)

source.take('Index') >> client_contact_lookup >> sink.put('client_contact_id')
StaticSource('Other') >> sink.put('type')
# `take_each` is a custom method for `RepeaterSink` to specify multiple source columns.
# Each column will be taken as part of a separate "row" when sent down the pipeline.
source.take_each('Phone 1', 'Phone 2') >> (SkipIf.value == '') >> sink.put('display').then >>\
    partial(re.compile('[^0-9]').sub, '') >> sink.put('search')
# (The `then` construct is not unique to `RepeaterSink`, and can be used in any situation where 
# you want to put the same value in the sink in multiple places, possibly with additional 
# transformation the second time.)

process_all(sink)
