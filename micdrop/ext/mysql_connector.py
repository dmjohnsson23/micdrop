"""
Extension integrating with the mysql-connector-python library. `https://dev.mysql.com/doc/connector-python/en/`_

Be aware that this library assumes the use of trusted input and therefore does not attempt to 
guard against SQL injection, other than "accidental" injection such as using a keyword as a
table or column name. (The actual values inserted by the various sinks are parameterized and
therefore safe from injection; this note refers mainly to column and table names.)

You will probably want to use pooled connection to ensure you have enough connections. You will
need separate connections for the source and sink if doing a MySQL-to-MySQL migrations, even
if the databases are on the same server. You'll also need another separate connection to pass
to any LazyLookupTable pipeline items you use, meaning you may need as many as three 
connections to ensure they don't clash with one another.
"""
from ..base import Source, PipelineItem
from ..sink import Sink
from ..transformers import Lookup
from ..collect import CollectArgsKwargs
from typing import Union
from functools import lru_cache

class QuerySource(Source):
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



class TableSource(QuerySource):
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


def _build_set_statements(names, values, *, actions:dict={}, default_action='COALESCE'):
    set_statements = []
    for name, value in zip(names, values):
        func = actions.get(name, default_action)
        if func == 'COALESCE':
            set_statements.append(f'`{name}` = COALESCE({value}, `{name}`)')
        elif func == 'OVERWRITE_NULLS':
            set_statements.append(f'`{name}` = COALESCE(`{name}`, {value})')
        elif func == 'ALWAYS_OVERWRITE':
            set_statements.append(f'`{name}` = {value}')
        elif func == 'KEEP_EXISTING':
            set_statements.append(f'`{name}` = `{name}`')
        elif func == 'APPEND':
            set_statements.append(f'`{name}` = CONCAT(`{name}`, " ", {value})')
        elif func == 'APPEND_LINE':
            set_statements.append(f'`{name}` = CONCAT(`{name}`, "\n", {value})')
        elif func == 'PREPEND':
            set_statements.append(f'`{name}` = CONCAT({value}, " ", `{name}`)')
        elif func == 'PREPEND_LINE':
            set_statements.append(f'`{name}` = CONCAT({value}, "\n", `{name}`)')
        elif func == 'ADD':
            set_statements.append(f'`{name}` = `{name}` + {value}')
        else:
            raise ValueError(f'Unknown function for update clause: {func}')
    return','.join(set_statements)

class TableInsertSink(Sink):
    """
    Sink to insert into a specific table. This should be much more performant than `TableSink`,
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

    def process(self, source, *, buffer_size:int = 100, on_duplicate_key_update:Union[str,bool]=False, on_duplicate_key_actions:dict={}, on_duplicate_key_default_action="COALESCE"):
        """
        :param buffer_size: The number of results to hold in memory before inserting into the database
        :param on_duplicate_key_update: And ``ON DUPLICATE UPDATE`` clause to append, or ``True`` to 
            generate one automatically.
        :param on_duplicate_key_actions: If provided, map column names to the function that should be used 
            the ``ON DUPLICATE KEY UPDATE`` clause.

            - COALESCE (default): Update the existing value only if the new value is not null
            - OVERWRITE_NULLS: Update the existing value only if the existing value is null
            - ALWAYS_OVERWRITE: Always overwrite the existing value, even if the new value is null
            - KEEP_EXISTING: Never overwrite the existing value for any reason
            - APPEND: Concat the existing and new values with a space
            - APPEND_LINE: Concat the existing and new values with a newline
            - PREPEND: Concat the existing and new values with a space
            - PREPEND_LINE: Concat the existing and new values with a newline
            - ADD: Sum the previous and new values
        :param on_duplicate_key_default_action: Allows overriding the default action value for columns
            not supplied in `on_duplicate_key_actions`.
        """
        query = f"""
            INSERT INTO `{self.db_name}`.`{self.table_name}` 
            (`{'`, `'.join(self._puts.keys())}`)
            VALUES
            ({', '.join(['%s' for _ in range(len(self._puts))])})
        """
        if on_duplicate_key_update is True:
            set_statements = _build_set_statements(
                self._puts.keys(), 
                map("VALUES(`{}`)".format, self._puts.keys()), 
                actions=on_duplicate_key_actions, 
                default_action=on_duplicate_key_default_action
            )
            query = f"{query} ON DUPLICATE KEY UPDATE {set_statements};"
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

class TableUpdateSink(Sink):
    """
    Update-only sink for when you know the relevant items already exist in the database. This
    has more database round-trips than `TableInsertSink`, but still fewer than `TableSink`.
    
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
        :param key_column: The column to use as a key in the update clause (Should be the primary
            key or another unique column.)
        """
        super().__init__()
        self.db_cursor = db_cursor
        self.db_name = db_name
        self.table_name = table_name
        self.key_column = key_column

    def process(self, source, *, actions:dict={}, default_action="COALESCE"):
        set_statements = _build_set_statements(
                self._puts.keys(), 
                ['%s'] * len(self._puts), 
                actions=actions, 
                default_action=default_action
            )
        query = f"""
            UPDATE `{self.db_name}`.`{self.table_name}` SET
            {set_statements}
            WHERE `{self.key_column}` = %s;
        """
        for row in super().process(source):
            self.db_cursor.execute(query, [*(row[key] for key in self._puts.keys()), row[self.key_column]])
            yield row


class QuerySink(Sink):
    """
    Sink to call a parameterized query with put values as parameters
    """
    def __init__(self, db_cursor, query, multi_query=False):
        """
        :param db_cursor: A mysql-connector cursor object (This will be in-use for the entire 
            duration the pipeline is running, so you should not attempt to use it while the 
            pipeline is processing, though you may re-use it afterward as it will not be closed)
        :param query: The query to run. Use "%(name)s" to represent parameters.
        """
        super().__init__()
        self.db_cursor = db_cursor
        self.query = query
        self.multi_query = multi_query

    def process(self, source):
        for row in super().process(source):
            result = self.db_cursor.execute(self.query, row, self.multi_query)
            if self.multi_query:
                query_values = [r.fetchall() for r in result if r.with_rows]
            elif self.db_cursor.with_rows:
                query_values = self.db_cursor.fetchall()
            else:
                query_values = None
            yield row, query_values


class TableSink(Sink):
    """
    General sink for MySQL tables that can check if an item exists and intelligently merge items
    in Python. Most use cases should be satisfied by the more efficient `TableInsertSink`,
    or `TableUpdateSink`; this class makes two database round-trips per row.
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
        :param key_column: The column to use as a key to fetch the "check" row, as well as to 
            target the UPDATE statement if used. Does not necessarily have to be unique, though
            in most cases probably should be.
        """
        super().__init__()
        self.db_cursor = db_cursor
        self.db_name = db_name
        self.table_name = table_name
        self.key_column = key_column

    def process(self, source, *, do_updates=True, update_actions:dict={}, update_default_action="COALESCE"):
        query_select = f"""
            SELECT *
            FROM `{self.db_name}`.`{self.table_name}` 
            WHERE `{self.key_column}` = %s
            LIMIT 1;
        """
        query_insert = f"""
            INSERT INTO `{self.db_name}`.`{self.table_name}` 
            (`{'`, `'.join(self._puts.keys())}`)
            VALUES
            ({', '.join(['%s' for _ in range(len(self._puts))])})
        """
        if do_updates:
            set_statements = _build_set_statements(
                self._puts.keys(), 
                ['%s'] * len(self._puts), 
                actions=update_actions, 
                default_action=update_default_action
            )
            query_update = f"""
                UPDATE `{self.db_name}`.`{self.table_name}` SET
                {set_statements}
                WHERE `{self.key_column}` = %s;
            """
        for row in super().process(source):
            self.db_cursor.execute(query_select, (row[self.key_column],))
            selected = self.db_cursor.fetchall()
            if selected and do_updates:
                # An existing value was found and we want to update it
                self.db_cursor.execute(query_update, [*(row[key] for key in self._puts.keys()), row[self.key_column]])
            elif not selected:
                # No existing value was found
                self.db_cursor.execute(query_insert, [row[key] for key in self._puts.keys()])
            yield row

class LookupTable(Lookup):
    """
    Pipeline item to look up a value in a table. This will fetch and store the entire lookup
    table in memory, so should only be used for relatively small lookup tables.
    """
    def __init__(self, db_cursor, db_name, table_name, key_column, value_column, *, convert_keys=str):
        """
        :param db_cursor: A mysql-connector cursor object (This may be used immediately, not
            during pipeline processing, so it is safe to re-use the source or sink cursor)
        :param db_name: The name of the database to connect to (Not safe against SQL injection; 
            make sure it comes from a trusted source)
        :param table_name: The name of the table or view to pull data from (Not safe against SQL 
            injection; make sure it comes from a trusted source)
        :param key_column: The column that will be the key of the lookup table (must be unique)
        :param value_column: The column that will be the value of the lookup table
        :param convert_keys: If provided, a callable that will be used to convert all lookup keys before
            use. May be useful when typing is inconsistent or more flexible typing is needed.Set to None
            to do no conversion.
        """
        query = f"""
            SELECT `{key_column}`, `{value_column}`
            FROM `{db_name}`.`{table_name}`
        """
        db_cursor.execute(query)
        super().__init__(dict(db_cursor.fetchall()), convert_keys=convert_keys)

class CollectQuery(CollectArgsKwargs):
    """
    Run a query and return the results. Allows you to put multiple values for complex queries.

    Example::

        with CollectQuery("SELECT * FROM the_table WHERE thing1 = %(thing1)s OR thing2 = %(thing2)s") as query:
            source.take('thing1') >> query.put('thing1')
            source.take('thing2') >> query.put('thing2')
            query.take(0).take('thing3') >>
    """
    _value = None
    def __init__(self, db_cursor, query):
        """
        :param db_cursor: A mysql-connector cursor object (This is used during the pipeline,
            so should be different from the cursor used in the source or sink.)
        :param query: The query to execute. You may use "%s" or "%(name)s as placeholders for
            the put values; but you must exclusively use one style or the other.
        """
        self.query = query
        self.db_cursor = db_cursor

    def get(self):
        if self._value is None:
            args, kwargs = super().get()
            if args and kwargs:
                raise RuntimeError('CollectQuery should not mix positional and named parameters; use one style or the other')
            self.db_cursor.execute(self.query, args or kwargs)
            self.value = self.db_cursor.fetchall()
        return self._value
    

class Query(PipelineItem):
    """
    Run a query and return the results
    """
    def __init__(self, db_cursor, query):
        """
        :param db_cursor: A mysql-connector cursor object (This is used during the pipeline,
            so should be different from the cursor used in the source or sink.)
        :param query: The query to execute. You may use a single "%s" as a placeholder for
            whatever value this pipeline item receives.
        """
        self.query = query
        self.db_cursor = db_cursor

    @lru_cache(32)
    def process(self, value):
        self.db_cursor.execute(self.query, (value,))
        return self.db_cursor.fetchall() or None
    
class QueryValue(Query):
    """
    Run a query and return a single value from it. 
    """

    @lru_cache(32)
    def process(self, value):
        self.db_cursor.execute(self.query, (value,))
        rows = self.db_cursor.fetchall()
        if rows:
            return rows[0][0]
        else:
            return None
    
class FetchValue(QueryValue):
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
        super().__init__(db_cursor, f"""
            SELECT `{value_column}`
            FROM `{db_name}`.`{table_name}`
            WHERE `{key_column}` = %s
        """)

class QueryRow(Query):
    """
    Run a query and return a single row from it. 
    """

    @lru_cache(32)
    def process(self, value):
        self.db_cursor.execute(self.query, (value,))
        row = self.db_cursor.fetchall()
        if row:
            return dict(zip(self.db_cursor.column_names, row[0]))
        else:
            return None
        
class FetchRow(QueryRow):
    """
    Pipeline item to fetch a row by ID, with some caching.
    """
    def __init__(self, db_cursor, db_name, table_name, key_column):
        """
        :param db_cursor: A mysql-connector cursor object (This is used during the pipeline,
            so should be different from the cursor used in the source or sink.)
        :param db_name: The name of the database to connect to (Not safe against SQL injection; 
            make sure it comes from a trusted source)
        :param table_name: The name of the table or view to pull data from (Not safe against SQL 
            injection; make sure it comes from a trusted source)
        :param key_column: The column that will be the key of the lookup table (must be unique)
        """
        super().__init__(db_cursor, f"""
            SELECT *
            FROM `{db_name}`.`{table_name}`
            WHERE `{key_column}` = %s
            LIMIT 1
        """)
        
class QueryColumn(Query):
    """
    Run a query and return a list of multiple values from it. 
    """

    @lru_cache(32)
    def process(self, value):
        self.db_cursor.execute(self.query, (value,))
        rows = self.db_cursor.fetchall()
        if rows:
            return [row[0] for row in rows]
        else:
            return None
        
class FetchColumn(QueryColumn):
    """
    Pipeline item to fetch a an array of values, with some caching.
    """
    def __init__(self, db_cursor, db_name, table_name, key_column, value_column):
        """
        :param db_cursor: A mysql-connector cursor object (This is used during the pipeline,
            so should be different from the cursor used in the source or sink.)
        :param db_name: The name of the database to connect to (Not safe against SQL injection; 
            make sure it comes from a trusted source)
        :param table_name: The name of the table or view to pull data from (Not safe against SQL 
            injection; make sure it comes from a trusted source)
        :param key_column: The column that will be the key of the lookup table
        :param value_column: The column that will be the value fetched
        """
        super().__init__(db_cursor, f"""
            SELECT `{value_column}`
            FROM `{db_name}`.`{table_name}`
            WHERE `{key_column}` = %s
        """)