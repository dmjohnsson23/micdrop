"""
Extension integrating with the mysql-connector-python library. `https://dev.mysql.com/doc/connector-python/en/`_

Be aware that this library assumes the use of trusted input and therefore does not attempt to 
guard against SQL injection, other than "accidental" injection such as using a keyword as a
table or column name. (The actual values inserted by the various sinks are parameterized and
therefore safe from injection; this note refers mainly to column and table names.)

You will probably want to use pooled connection to ensure you have enough connections. You will
need separate connections for the source and sink if doing a MySQL-to-MySQL migrations, even
if the databases are on the same server. You'll also need another separate connection to pass
to any MySqlLazyLookupTable pipeline items you use, meaning you may need as many as three 
connections to ensure they don't clash with one another.
"""
from ..source import Source
from ..sink import Sink
from ..pipeline import Lookup, PipelineItem
from typing import Union
from functools import lru_cache

class MySqlSource(Source):
    """
    Source to pull from a specific MySql table
    """
    def __init__(self, db_cursor, query, params=None):
        """
        :param db_cursor: A mysql-connector cursor object (This will be in-use for the entire 
            duration the pipeline is running, so you should not attempt to use it while the 
            pipeline is processing, though you may re-use it afterward as it will not be closed)
        :param query: The query to execute to pull the data
        """
        self.db_cursor = db_cursor
        self.params = params
        self.query = query
        self._iter = None

    def get(self, key):
        return self._current_value[key]

    def next(self):
        try:
            self._current_value = dict(zip(self._columns, next(self._iter)))
            self._current_index += 1
            return True
        except StopIteration:
            return False
    
    def open(self):
        self.db_cursor.execute(self.query, self.params)
        self._columns = self.db_cursor.column_names
        self._iter = iter(self.db_cursor)
        self._current_index = -1



class MySqlTableSource(MySqlSource):
    """
    Source to pull from a specific MySql table
    """
    def __init__(self, db_cursor, db_name, table_name, condition=None):
        """
        :param db_cursor: A mysql-connector cursor object (This will be in-use for the entire 
            duration the pipeline is running, so you should not attempt to use it while the 
            pipeline is processing, though you may re-use it afterward as it will not be closed)
        :param db_name: The name of the database to connect to (Not safe against SQL injection; 
            make sure it comes from a trusted source)
        :param table_name: The name of the table or view to pull data from (Not safe against SQL 
            injection; make sure it comes from a trusted source)
        :param condition: An optional condition to add as a WHERE clause
        """
        query = f"SELECT * FROM `{db_name}`.`{table_name}`"
        if condition:
            query = f"{query} WHERE {condition}"
        super().__init__(db_cursor, query)




class MySqlTableInsertSink(Sink):
    """
    Sink to insert into a specific table. This should be much more performant than `MySqlTableSink`,
    but has fewer features.
    """
    def __init__(self, db_cursor, db_name, table_name):
        """
        :param db_cursor: A mysql-connector cursor object (This will be in-use for the entire 
            duration the pipeline is running, so you should not attempt to use it while the 
            pipeline is processing, though you may re-use it afterward as it will not be closed)
        :param db_name: The name of the database to connect to (Not safe against SQL injection; 
            make sure it comes from a trusted source)
        :param table_name: The name of the table or view to pull data from (Not safe against SQL 
            injection; make sure it comes from a trusted source)
        """
        super().__init__()
        self.db_cursor = db_cursor
        self.db_name = db_name
        self.table_name = table_name

    def process(self, source, *, buffer_size:int = 100, on_duplicate_key_update:Union[str,bool]=False):
        """
        :param buffer_size: The number of results to hold in memory before inserting into the database
        :param on_duplicate_key_update: And ``ON DUPLICATE UPDATE`` clause to append, or ``True`` to 
            generate one automatically.
        """
        query = f"""
            INSERT INTO `{self.db_name}`.`{self.table_name}` 
            (`{'`, `'.join(self._puts.keys())}`)
            VALUES
            ({', '.join(['%s' for _ in range(len(self._puts))])})
        """
        if on_duplicate_key_update is True:
            query = f"""{query}
                ON DUPLICATE KEY UPDATE
                {','.join([f'`{name}` = COALESCE(VALUES(`{name}`), `{name}`)' for name in self._puts.keys()])};
            """
        elif on_duplicate_key_update:
            query = f"{query} ON DUPLICATE KEY UPDATE {on_duplicate_key_update}"
        buffer = []
        for row in super().process(source):
            buffer.append([row[key] for key in self._puts.keys()])
            yield row
            if len(buffer) >= buffer_size:
                self.db_cursor.executemany(query, buffer)
                buffer = []
        # Send any remaining buffer items
        if buffer:
            self.db_cursor.executemany(query, buffer)

class MySqlTableUpdateSink(Sink):
    """
    Update-only sink for when you know the relevant items already exist in the database. This
    has more database round-trips than `MySqlTableInsertSink`, but still fewer than
    `MySqlTableSink`.
    """
    def __init__(self, db_cursor, db_name, table_name, key_column):
        """
        :param db_cursor: A mysql-connector cursor object (This will be in-use for the entire 
            duration the pipeline is running, so you should not attempt to use it while the 
            pipeline is processing, though you may re-use it afterward as it will not be closed)
        :param db_name: The name of the database to connect to (Not safe against SQL injection; 
            make sure it comes from a trusted source)
        :param table_name: The name of the table or view to pull data from (Not safe against SQL 
            injection; make sure it comes from a trusted source)
        :param key_column: The column to use as a key in the update clause (Should generally be
            the primary key or another unique column)
        """
        super().__init__()
        self.db_cursor = db_cursor
        self.db_name = db_name
        self.table_name = table_name
        self.key_column = key_column

    def process(self, source):
        query = f"""
            UPDATE `{self.db_name}`.`{self.table_name}` SET
            {','.join([f'`{name}` = COALESCE(%s, `{name}`)' for name in self._puts.keys()])}
            WHERE `{self.key_column}` = %s;
        """
        for row in super().process(source):
            self.db_cursor.execute(query, [*(row[key] for key in self._puts.keys()), row[self.key_column]])
            yield row



class MySqlTableSink(Sink):
    """
    General sink for MySQL tables that can check if an item exists and intelligently merge items
    in Python. Most use cases should be satisfied by the more efficient `MySqlTableInsertSink`
    or `MySqlTableUpdateSink`; this class makes two database round-trips per row.
    """
    pass

class MySqlLookupTable(Lookup):
    """
    Pipeline item to look up a value in a table. This will fetch and store the entire lookup
    table in memory, so should only be used for relatively small lookup tables.
    """
    def __init__(self, db_cursor, db_name, table_name, key_column, value_column):
        """
        :param db_cursor: A mysql-connector cursor object (This may be used immediately, not
            during pipeline processing, so it is safe to re-use the source or sink cursor)
        :param db_name: The name of the database to connect to (Not safe against SQL injection; 
            make sure it comes from a trusted source)
        :param table_name: The name of the table or view to pull data from (Not safe against SQL 
            injection; make sure it comes from a trusted source)
        :param key_column: The column that will be the key of the lookup table (must be unique)
        :param value_column: The column that will be the value of the lookup table
        """
        query = f"""
            SELECT `{key_column}`, `{value_column}`
            FROM `{db_name}`.`{table_name}`
        """
        db_cursor.execute(query)
        super().__init__(dict(db_cursor.fetchall()))


class MySqlLazyLookupTable(PipelineItem):
    """
    Pipeline item to look up a value in a table. Keeps a small LRU cache of results, but
    otherwise looks up values on-the-fly rather than fetching all at once and keeping
     the entire lookup in memory. This is useful for larger lookup tables.
    """
    def __init__(self, db_cursor, db_name, table_name, key_column, value_column):
        """
        :param db_cursor: A mysql-connector cursor object (This is used during the pipeline,
            so should be different from the cursor used in the source or sink.)
        :param db_name: The name of the database to connect to (Not safe against SQL injection; 
            make sure it comes from a trusted source)
        :param table_name: The name of the table or view to pull data from (Not safe against SQL 
            injection; make sure it comes from a trusted source)
        :param key_column: The column that will be the key of the lookup table (must be unique)
        :param value_column: The column that will be the value of the lookup table
        """
        self._query = f"""
            SELECT `{value_column}`
            FROM `{db_name}`.`{table_name}`
            WHERE `{key_column}` = %s
        """
        self.db_cursor = db_cursor

    @lru_cache(32)
    def process(self, value):
        self.db_cursor.execute(self._query, (value,))
        return self.db_cursor.fetchall()[0][0]
