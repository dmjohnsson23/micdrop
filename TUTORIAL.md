# MIC Drop Tutorial

To build a migration script with MIC Drop, you'll implement one or more **pipelines** that you data will pass through. These pipelines will perform transformations on your data.

To make a pipeline, you'll first need two things: a **Source** and a **Sink**. The source if the data source you want to extract data from, and the sink will be the destination or target which will receive the data. The source will contain several rows or records, which will be sent through the pipeline for processing before being added to the sink.

Your source and sink can each be any number of things: a database table, a CSV file, a collection of XML or JSON files, etc. For this tutorial, lets assume we have a CSV file: `CONTACTS.CSV`, and we want to import this into an SQL database.

## The Basics

Let's start by getting things set up.

```python
import sqlalchemy as sql
from micdrop import *
from micdrop.ext.csv import *
from micdrop.ext.sql_alchemy import *

# We're using the SQLAlchemy extension for MIC Drop, so we need to set up the SQLAlchemy engine
engine = sql.create_engine(sql.URL(
    "mysql+pymysql",
    mysql_user,
    mysql_password,
    mysql_host,
    None,
    mysql_db,
    {'charset': 'utf8mb4'}))
meta = sql.MetaData()
```

Now we can start declaring our first pipeline's source and sink.

```python
source = CSVSource('CONTACTS.CSV')
contacts_table = sql.Table('contacts', meta, autoload_with=engine)
sink = TableInsertSink(engine, contacts_table)
```

Now, we're all set to start mapping fields. Many fields can likely be mapped one-to-one without any conversion needed. For example:

```python
source.take('FIRST NAME') >> sink.put('first_name')
source.take('MIDDLE NAME') >> sink.put('middle_name')
source.take('LAST NAME') >> sink.put('last_name')
```

This code instructions the pipeline to extract the `FIRST NAME`, `MIDDLE NAME`, and `LAST NAME` columns from our CSV, and to place the values from those into the `first_name`, `middle_name`, and `last_name` columns of our database's `contacts` table.

But, of course, many fields won't be quite that easy, and will require some extra conversion before they can be migrated. For example, our birth dates may be in MM/DD/YYYY format in the CSV. We'll need to parse these out into real dates.

```python
source.take('BIRTH DATE') >> ParseDate('%m/%d/%Y') >> sink.put('dob')
```

We can also perform basic lookups for a set of known values. Unknown values will be treated like `None`.

```python
source.take('GENDER') >> {'M': 'male', 'F': 'female'} >> source.put('sex')
source.take('MARRIED?') >> {'Y': 'married'} >> source.put('marital_status')
```

We can also use Python functions in the pipeline to perform conversions.

```python
source.take('AGE') >> int >> sink.put('age')
```

There are fields in our CSV that don't align well with any fields in our sink. But, we don't want to lose that data! This is a good opportunity to look at **Collectors**. This are pipeline items that aggregate one or more source field into a single destination field.

```python
with CollectDict() as extras:
    source.take('FAVORITE COLOR') >> extras.put('FAVORITE COLOR')
    source.take('FAVORITE NUMBER') >> extras.put('FAVORITE NUMBER')
    source.take('FAVORITE MUSIC GENRE') >> extras.put('FAVORITE MUSIC GENRE')
    extras >> JoinKeyValue(': ', '\n') >> sink.put('notes')
```

This code will result in the `notes` column containing text like this:

```
FAVORITE COLOR: Sea Green
FAVORITE NUMBER: 23
FAVORITE MUSIC GENRE: Folk
```

We've got a basic pipeline built; now let's run it.

```python
process_all(sink)
```

## Data Normalization

We can also deal with more complex structural differences in our data. In our example, the source CSV has the person's address inline, and phone numbers as separate columns for home, work and cell. However, the destination system has an entirely separate table for storing addresses and phone numbers. We'll implement those as entirely new pipelines, pulling from the same source file.

But, before that, we need to make one small change to our previous pipeline. The source CSV doesn't provide any kind of unique identifier for our contacts. We'll need that if we are to deal with the foreign key assignments for these child tables. Fortunately, the CSV extension *does* expose row numbers.

One option would be to use these directly as the primary key:

```python
source.take_index() >> sink.put('id')
```

While this option is great if we are importing into a blank database, and the table's primary key is a simple integer value. However, since we're importing into an existing database, we'll have to get a bit more creative.

Let's return to the top of the script and make a temporary change to the `contacts` table.

```python
with engine.begin() as conn:
    conn.execute(text(f'''ALTER TABLE `{mysql_db}`.contacts
        ADD tmp_migrated_id INT NULL,
        ADD INDEX (tmp_migrated_id)
    '''))
```

We'll delete this temporary column once the migration is complete, but for now it will give us a way to reference our newly added items in other pipelines. Let's put our row number in this new column:

```python
source.take_index() >> sink.put('tmp_migrated_id')
```

Now, we can use that same row number to perform a lookup when we start our new pipeline for address data.

```python
source = CSVSource('CONTACTS.CSV')
addresses_table = sql.Table('contact_addresses', meta, autoload_with=engine)
sink = TableInsertSink(engine, phones_table)

source.take_index() >> FetchValue(engine, contacts_table, 'tmp_migrated_id', 'id') >> sink.put('contact_id')
source.take('ADDR LINE 1') >> sink.put('line1')
source.take('ADDR LINE 2') >> sink.put('line2')
source.take('CITY') >> sink.put('city')
source.take('STATE') >> sink.put('state')
StaticSource('USA') >> sink.put('country')
CollectList(
    source.take('ZIP CODE'),
    source.take('ZIP CODE +4')
) >> JoinDelimited('-') >> sink.put('postal_code')

process_all(sink)
```

You'll notice the `FetchValue` pipeline item. This item will query the contacts table by the migrated ID to get the actual ID. We don't have to implement the lookup logic ourselves!

You'll also notice another new concept here: what is this `StaticSource` thing? Our destination application requires a value for the `country` field, but our source data does not provide this information. The `StaticSource` will provide a value of `"USA"` for every row of the main source that we can put in that hidden field. Problem solved!

You've seen collectors already, but here we are using a new one: `CollectList`. We're also using a different syntax to build the collector than we did before: adding the sources to the constructor of the collector rather than using a `with` block. Both syntaxes are valid; use whichever you find more readable.

Great, now let's do the phone numbers. These will add even more unique normalization challenges we'll have to overcome.

```python
source = CSVSource('CONTACTS.CSV')
phones_table = sql.Table('contact_phones', meta, autoload_with=engine)
sink = RepeaterSink(TableInsertSink(engine, phones_table))

source.take_index() >> FetchValue(engine, contacts_table, 'tmp_migrated_id', 'id') >> sink.put('contact_id')
StaticSource(('home', 'work', 'cell')) >> sink.put_each('type')
CollectList(
    source.take("HOME PHONE"),
    source.take("WORK PHONE"),
    source.take("CELL PHONE"),
) >> sink.put_each('number')

process_all(sink)
```

Wow, this looks all kinds of different! But, not really.

We *could* have run a separate pipeline for each of the three phone types. That would have been easy. But, that means iterating over the source three times instead of one.

The `RepeaterSink` is a wrapper around a sink that adds a new `put_each` method. This method expects to receive an iterable rather than a single value, and for all iterable fields in the source, it will insert a separate row in the sink. So, in this example, three rows are inserted into the sink for every one in the source. The first of these three uses the first value put into `type` and `number`, the second uses the second, and the third of course uses the third value. However, since `contact_id` was put using the normal method, it will just use the normal single value for all three inserted rows. This gets us the phone numbers in a single pass.

## Reading and Writing Loose Files

There might be files associated with the records in your source. Our example contacts CSV might have photos associated with our contacts. There are multiple ways of dealing with files, both on the source side and the destination side. We'll explore several here.

### Data First, Copy Files

This method is the best method to use if:

* You have an index file pointing to the files you want to read
* The destination expects the files to continue living in the filesystem, rather than the database

This is perhaps the most simple and straightforward approach.

```python
source = CSVSource('CONTACTS.CSV')
photos_table = sql.Table('contact_photos', meta, autoload_with=engine)
sink = TableInsertSink(engine, photos_table)

source.take_index() >> FetchValue(engine, contacts_table, 'tmp_migrated_id', 'id') >> sink.put('contact_id')
source.take('PHOTO FILENAME') >> CopyFile('/path/to/photos/', '/path/to/app/data/') >> sink.put('filename')

process_all(sink)
```

The `CopyFile` pipeline item does all the magic: it takes a filename as input, copies it from the source directory to the destination directory, and returns the new filename. The pipeline item also supports an optional third argument which can be another pipeline used to calculate the file name.

### Data First, Load Files

This method is the best method to use if:

* You have an index file pointing to the files you want to read
* Either you need to perform processing on the file contents first,
* Or the sink wants to store the file directly, such as in a database table, rather than on the filesystem

This is again quite straightforward.

```python
source = CSVSource('CONTACTS.CSV')
photos_table = sql.Table('contact_photos', meta, autoload_with=engine)
sink = TableInsertSink(engine, photos_table)

source.take_index() >> FetchValue(engine, contacts_table, 'tmp_migrated_id', 'id') >> sink.put('contact_id')
source.take('PHOTO FILENAME') >> ReadFile('/path/to/photos/') >> sink.put('blob')

process_all(sink)
```

Here, `ReadFile` will get the read the file identified by the input filename, passing along the file's contents as a byte string. Pass `True` as the second argument to read the file in text mode instead.

### Files First, Copy Files

This method is the best method to use if:

* The file's name or location provides enough information to do any necessary associations with data
* The destination expects the files to continue living in the filesystem, rather than the database

If our files are named according to a specific naming convention of `{{lastname}}_{{firstname}}.jpg`, we can take advantage of this for a lookup. The `FilesSource` will iterate all files in a directory, exposing the file names as the index.

```python
from os.path import basename, splitext

source = FilesSource('/path/to/photos')
photos_table = sql.Table('contact_photos', meta, autoload_with=engine)
sink = TableInsertSink(engine, photos_table)

with source.take_index() >> basename >> splitext >> Take(0) >> SplitDelimited('_') as file_name:
    with CollectQueryValue(engine,
        "SELECT id FROM contacts WHERE first_name LIKE :first and last_name LIKE :last LIMIT 1"
    ) as collect:
        file_name.take(0) >> collect.put('last')
        file_name.take(1) >> collect.put('first')
        collect >> SkipIf.value.is_(None) >> sink.put('contact_id')
source.take_index() >> CopyFile('/path/to/photos/', '/path/to/app/data/') >> sink.put('filename')

process_all(sink)
```

This code does a few interesting things:

* It takes the same value (the index) twice: once to look up the contact, and once to. This is allowed!
* Uses a `Take` inline as a pipeline item to extract the value from the `splitext` function.
* Uses an incomplete pipeline as a context manager using a `with` statement. This prevents code duplication--the pipeline can "split".
* The `CollectQueryValue` collector. This collector uses the values which are put as parameters for an SQL query. The query can be entered as a string, or constructed using SQLAlchemy's DSL.

### Files First, Load Files

This method is the best method to use if:

* The file's name or location provides enough information to do any necessary associations with data
* Either you need to perform processing on the file contents first,
* Or the sink wants to store the file directly, such as in a database table, rather than on the filesystem

This is simply a combination of the previous two methods, so should look familiar.

```python
from os.path import basename, splitext

source = FilesSource('/path/to/photos')
photos_table = sql.Table('contact_photos', meta, autoload_with=engine)
sink = TableInsertSink(engine, photos_table)

with source.take_index() >> basename >> splitext >> Take(0) >> SplitDelimited('_') as file_name:
    with CollectQueryValue(engine,
        "SELECT id FROM contacts WHERE first_name LIKE :first and last_name LIKE :last LIMIT 1"
    ) as collect:
        file_name.take(0) >> collect.put('last')
        file_name.take(1) >> collect.put('first')
        collect >> SkipIf.value.is_(None) >> sink.put('contact_id')
source.take_index() >> ReadFile('/path/to/photos/') >> sink.put('blob')

process_all(sink)
```

### Extract Files From Data

This method is the best method to use if:

* The file contents are stored in the source data, such as in a database table, rather than the filesystem
* Either you need to perform processing on the file contents,
* Or the sink wants to store the file on the filesystem

Let imagine our contact photos are actually stored as base64-encoded JPEGs in our main contacts CSV instead of as separate files. Let's extract them.

```python
from base64 import decode

source = CSVSource('CONTACTS.CSV')
photos_table = sql.Table('contact_photos', meta, autoload_with=engine)
sink = RepeaterSink(TableInsertSink(engine, photos_table))

source.take_index() >> FetchValue(engine, contacts_table, 'tmp_migrated_id', 'id') >> sink.put('contact_id')
source.take('PHOTO BASE64') >> decode >> WriteFile(
    '/path/to/app/data/' # Directory to save files in
    source.take_index() >> "{}.jpeg" # Filename will be '{{row_number}}.jpeg'
) >> source.put('filename')

process_all(sink)
```

This pipeline will read the image data from the CSV, decode it, then save it to the app's data directory with the row number as the filename. You can use any pipeline to define the name, or even leave it blank to let MIC Drop choose a name for you.