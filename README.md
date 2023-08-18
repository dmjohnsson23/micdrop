# MIC Drop (Migrate, Import, Convert)

Extensible framework/library to migrate data from source to another using a declarative interface. The library makes elaborate (and somewhat unconventional) use of operator overloading to abstract away some magic and allow you to focus on the important part: mapping data.

## Terminology

* Source: A source of data to be transformed, consisting of multiple rows
* Row: A single object in a source (Could be a literal Python object, a row of a CSV file or relational database table, etc... )
* Take: The beginning of a pipeline, which expects to receive a single value from each row in the source; or a branch in a pipeline
* Pipeline: A series of transformations that a value undergoes before being put in the sink
* Put: The end of a pipeline, or the merging of multiple pipelines in a collector
* Sink: The final destination in which rows are to be stored after their pipeline transformations

## Example

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
# (The shift operator is overloaded to represent flow direction in the pipeline)
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
values = source.take('PIPE SEPARATED') >> SplitDelimited('|')
values.take(0) >> sink.put('val1')
values.take(1) >> sink.put('val2')
values.take(2) >> sink.put('val3')
# An alternate syntax to split values, for readability
with source.take('PIPE SEPARATED') >> SplitDelimited('|') as values:
    values.take(0) >> sink.put('val1')
    values.take(1) >> sink.put('val2')
    values.take(2) >> sink.put('val3')
# Combine values
values = ListCollector()
source.take('THING 1') >> values.put()
source.take('THING 2') >> values.put()
values >> JoinDelimited(',') >> sink.put('things')
# The inverse syntax is also available and may be more readable in some circumstances
# (But don't mix-and-match the two on the same line)
values = ListCollector()
values.put() << source.take('THING 1')
values.put() << source.take('THING 2')
sink.put('things') << (lambda l: ','.join(l)) << values
# The context manager syntax can also be used if helpful
with ListCollector() as values:
    source.take('THING 1') >> values.put()
    source.take('THING 2') >> values.put()
    values >> (lambda l: ','.join(l)) >> sink.put('things')
# Some additional syntactic sugar makes each of the following sets of lines equivalent:
source.get('LAMBDAS') >> (lambda val: val) >> sink.put('lambdas')
source.get('LAMBDAS') >> Call(lambda val: val) >> sink.put('lambdas')
source.get('FUNCTIONS') >> some_func >> sink.put('functions')
source.get('FUNCTIONS') >> Call(some_func) >> sink.put('functions')
source.get('FORMAT STRINGS') >> "<p>{}</p>" >> sink.put('format_strings')
source.get('FORMAT STRINGS') >> Call("<p>{}</p>".format) >> sink.put('format_strings')
source.get('LOOKUP MAPPINGS') >> {'a':1, 'b':2} >> sink.put('lookup_mappings')
source.get('LOOKUP MAPPINGS') >> Lookup({'a':1, 'b':2}) >> sink.put('lookup_mappings')

# Import data from the source to the sink 
# (keyword arguments may differ from one sink type to another)
sink.process_all(source, on_duplicate_update=True)
```

## Extensibility

This library is designed for extensibility. Your can write your own sinks, sources, or pipeline items by extending `sink.Sink`, `source.Source`, and `pipeline.PipelineItem` respectively. You can also allow arbitrary classes to be used as pipeline items if you implement a method named `to_pipeline_source`, `to_pipeline_item`, or `to_pipeline_sink`.