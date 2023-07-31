# MIC Drop (Migrate, Import, Convert)

Extensible framework/library to migrate data from source to another using a declarative interface.

```python
from micdrop.pipeline import *
# Extensions exit for various different sources and sinks
from micdrop.ext.csv import CSVSource
from micdrop.ext.mysql_connector import MySqlTableInsertSink
from mysql.connector import connect
# Source and sink don't have to be the same type
source = CSVSource('/path/to/file.csv')
mysql = connect(**connection options)
sink = MySqlTableInsertSink(mysql.cursor(), 'database', 'table')

# Map straight across without any conversion
source.take('FIELD NAME') >> sink.put('new_field_name')
source.take('RECORD NUMBER') >> sink.put('id')
# Do conversions with regular python functions or lambdas
source.take('INT FIELD') >> int >> sink.put('int_field')
source.take('REVERSE ME') >> (lambda val: str(reversed(val))) >> sink.put('reversed')
# Or use pipeline operations from micdrop.pipeline
source.take('DATE OF BIRTH') >> ParseDate('%m/%d/%Y') >> FormatDate() >> sink.put('dob')
# Or with decorator syntax for more complex operations
@source.take('COMPLICATED STUFF')
def complicated_stuff(value):
    if value == 6:
        return 'six'
    else:
        return 'not six'
complicated_stuff >> sink.put('is_six')
# Split values
values = source.take('PIPE SEPARATED') >> lambda val: val.split('|')
values.take(0) >> sink.put('val1')
values.take(1) >> sink.put('val2')
values.take(2) >> sink.put('val3')
# Combine values
values = ListCollector()
source.take('THING 1') >> values.put()
source.take('THING 2') >> values.put()
values >> (lambda l: ','.join(l)) >> sink.put('things')
# The inverse syntax is also available and may be more readable in some circumstances
# (But don't mix-and-match the two on the same line)
values = ListCollector()
values.put() << source.take('THING 1')
values.put() << source.take('THING 2')
sink.put('things') << (lambda l: ','.join(l)) << values

# Import data from the source to the sink 
# (keyword arguments may differ from one sink type to another)
sink.process_all(source, on_duplicate_update=True)
```