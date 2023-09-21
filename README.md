# MIC Drop (Migrate, Import, Convert)

**Note: This library is still in development, and the API is expected to change with future improvements.**

Extensible framework/library to migrate data from source to another using a declarative interface. The library makes elaborate (and somewhat unconventional) use of operator overloading to abstract away some boilerplate and allow you to focus on the important part: mapping data.

At its core, the library's operation is quite simple: loop over the rows of the source data, perform some transformations, and output the transformed data to the sink.

## Terminology

* Source: A source of data to be transformed by additional pipeline items (all Pipeline Items are also Sources)
* Origin: The Source at the beginning of a pipeline, usually consisting of multiple Rows to be run through the pipeline
* Row: A single object in a Source, usually referring to the values output from the Origin (Could be a literal Python object, a row of a CSV file or relational database table, etc... )
* Take: Extracts a single value from a composite Source; either from the Origin, or at diverging pipelines
* Pipeline: A series of transformations that a value undergoes before being put in the Sink
* Pipeline Item: An object that is both a Source and a Put (e.g. it accepts some value, and outputs another value according to internal rules)
* Put: The end of a pipeline, or the merging of multiple pipelines in a Collector
* Collector: A pipeline convergence that accepts Puts and acts as a Source
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

## Documentation 

The various types of pipeline items available are reasonably well documented by the docblocks inside the source code, but there is currently no stand-alone documentation.

## Extensibility

This library is designed for extensibility. Your can write your own sinks, sources, or pipeline items by extending `sink.Sink`, `Source`, and `PipelineItem` respectively. You can also allow arbitrary classes to be used as pipeline items if you implement a method named `to_pipeline_source`, `to_pipeline_item`, or `to_pipeline_put` (Implement any one of the three).

## Priorities
1. Be easy to use
2. Handle large volumes of data without issue
3. Allow for graceful error recovery
4. Have acceptable performance (with the understanding that data migrations can take a long time when processing large amounts of data)

## TODOs

* Graceful error recovery (e.g., if a migration is running and an unexpected error occurs, there should be a way to retry or skip)
* Context managers on Origin Sources are currently conceptually used for a different purpose than context managers on all other sources; rectify this.
* Pipelines should function as reusable segments without needing the `PipelineSegment` class
    * Have do find a way to do this without breaking the cache/next mechanism, which I don't think we can do without
    * Maybe just always implicitly use `PipelineSegment`? (Would probably work, though the idempotency tokens could turn into quite the nasty mess of nested tuples)
* Generic "files" source/sink
    * Probably would need subclassed to be useful (e.g. to parse the files), but could provide the base for the XML classes below
* Looping "sub-pipelines" or "each" pipelines
    * E.g. if the source contains a list or other composite structures, and we want to modify the items without destructuring it
    * Could be done with pipeline segments
* Allow whole value to be sent to Sinks, not just Puts, and also allow indexes to be put for Sinks
* XML extension using ElementTree
    * Take using xpath
    * Allow multiple files or a singe file with multiple values (different source/sink classes probably)
* Build stand-alone documentation
* Async IO, to process multiple pipelines concurrently (Potential tool: https://github.com/bitcart/universalasync)