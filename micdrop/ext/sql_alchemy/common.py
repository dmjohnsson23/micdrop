from sqlalchemy import *

from micdrop.pipeline.base import Source
from ...pipeline import Source, PipelineItem,  Lookup, CollectDict
from ...sink import Sink
from typing import Union, Sequence, Mapping
from functools import lru_cache
from enum import Enum
__all__ = (
    'make_table', 'make_column', 'make_columns', 'UpdateAction',
    'QuerySource', 'TableSource',
    'QuerySink', 'TableInsertSink', 'TableUpdateSink', 'TableSink', 
    'LookupQuery', 'LookupTable',
    'CollectQuery', 'Query', 
    'CollectQueryValue', 'QueryValue', 'FetchValue', 
    'CollectQueryRow', 'QueryRow', 'FetchRow', 
    'CollectQueryColumn', 'QueryColumn', 'FetchColumn'
)

_meta = None
_tables = {}
def make_table(engine:Engine, table:Union[Table,str], *, db_name=None)->Table:
    global _meta, _tables
    if isinstance(table, Table):
        _tables[db_name,table.name] = table
        return table
    if (db_name,table) in _tables:
        return _tables[db_name,table]
    if _meta is None:
        _meta = MetaData()
    table = Table(table, _meta, autoload_with=engine, schema=db_name)
    _tables[db_name,table.name] = table
    return table

def make_column(table:table, column:Union[str,Column])->Column:
    if isinstance(column, Column):
        return column
    return table.columns[column]

def make_columns(table:table, columns:Union[str,Column,Sequence[Column],Sequence[str]])->Column:
    if isinstance(column, Sequence):
        return [make_column(table, c) for c in columns]
    return [make_column(table, columns)]


class UpdateAction(Enum):
    coalesce = 'COALESCE'
    overwrite_nulls = 'OVERWRITE_NULLS'
    always_overwrite = 'ALWAYS_OVERWRITE'
    keep_existing = 'KEEP_EXISTING'
    append = 'APPEND'
    append_line = 'APPEND_LINE'
    prepend = 'PREPEND'
    prepend_line = 'PREPEND_LINE'
    add = 'ADD'

    def func(self, column:Column, value):
        if self is UpdateAction.coalesce:
            return func.COALESCE(value, column)
        elif self is UpdateAction.overwrite_nulls:
            return func.COALESCE(column, value)
        elif self is UpdateAction.always_overwrite:
            return value
        elif self is UpdateAction.keep_existing:
            return column
        elif self is UpdateAction.append:
            return func.CONCAT(column, " ", value)
        elif self is UpdateAction.append_line:
            return func.CONCAT(column, "\n", value)
        elif self is UpdateAction.prepend:
            return func.CONCAT(value, " ", column)
        elif self is UpdateAction.prepend_line:
            return func.CONCAT(value, "\n", column)
        elif self is UpdateAction.add:
            return column + value


class QuerySource(Source):
    """
    Source to pull data from the database using a raw SQL query or an SQLAlchemy query object.

    Example usage::

        from sqlalchemy import *
        engine = create_engine("mysql+pymysql://user:pass@localhost/source_db?charset=utf8mb4")
        users = Table(...)
        source = QuerySource(engine, select(users).where(users.c.active == 1))
        # ...or...
        source = QuerySource(engine, 'SELECT * FROM users WHERE active = 1')
    """
    def __init__(self, engine:Engine, query:Union[str, Executable], params:dict=None, *, id_col=None, page_size=100):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param query: The query to execute to pull the data
        :param params: A sequence of parameters to use for the query
        :param id_col: A unique key to use as the index for this source.
        :param page_size: The number of items to fetch in each "page". Decrease this value if you
            have memory and/or timeout issues. Increase to make fewer round-trips to the database.
        """
        if isinstance(query, str):
            query = text(query)
        self.engine = engine
        self.params = params
        self.query = query
        self.id_col = id_col
        self.page_size = page_size
        self._current_page_offset = 0
        self._iter = None

    def keys(self):
        with self.engine.begin() as conn:
            result = conn.execute(
                # Use a subquery so we can apply limits (we don't need any values, just keys)
                select(self.query.subquery()).limit(1),
            self.params)
            keys = result.keys()
            result.one_or_none()
            return keys

    def next(self):
        try:
            self._current_value = next(self._iter)
            self._current_index += 1
        except StopIteration:
            # See if there is another page, then try again
            if self._next_page():
                return self.next()
            raise
    
    def get(self):
        return self._current_value
    
    def get_index(self):
        if self.id_col is None:
            return self._current_index
        if isinstance(self.id_col, str):
            return getattr(self._current_value, self.id_col)
        if isinstance(self.id_col, Column):
            return getattr(self._current_value, self.id_col.name)
        if isinstance(self.id_col, Sequence[str]):
            return tuple([getattr(self._current_value, col) for col in self.id_col])
        if isinstance(self.id_col, Sequence[Column]):
            return tuple([getattr(self._current_value, col.name) for col in self.id_col])
    
    def open(self):
        super().open()
        self._next_page()
        self._current_index = -1
    
    def _next_page(self):
        with self.engine.begin() as conn:
            result = conn.execute(
                # Use a subquery so we can apply limits for pagination
                select(self.query.subquery()).limit(self.page_size).offset(self._current_page_offset),
            self.params).fetchall()
            if result:
                self._current_page_offset += self.page_size
                self._iter = iter(result)
                return True
            return False
    
    def take(self, key, safe=False):
        if isinstance(key, int):
            return super().take(key, safe)
        else:
            return self.take_attr(key, safe)



class TableSource(QuerySource):
    """
    Source to pull from a specific database table

    Example usage::

        from sqlalchemy import *
        engine = create_engine("mysql+pymysql://user:pass@localhost/source_db?charset=utf8mb4")
        meta = MetaData()
        users = Table('users', meta, autoload_with=engine)
        source = TableSource(engine, users)
    """
    def __init__(self, engine:Engine, table:Union[Table,str], condition=None, *, page_size=100):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param table: The table to select from
        :param condition: An optional condition to add as a WHERE clause. This can be a string
            containing an SQL code snippet, or an SQLAlchemy column expression argument.
        :param page_size: The number of items to fetch in each "page". Decrease this value if you
            have memory and/or timeout issues. Increase to make fewer round-trips to the database.
        """
        self.table = make_table(engine, table)
        query = self.table.select()
        if condition:
            if isinstance(condition, str):
                condition = text(condition)
            query = query.where(condition)
        pk = self.table.primary_key
        id_col = [col.name for col in pk.columns] if pk is not None else None
        super().__init__(engine, query, id_col=id_col, page_size=page_size)
    
    def keys(self):
        self.table.c.keys()

    def _next_page(self):
        with self.engine.begin() as conn:
            result = conn.execute(
                # Slight optimization here vs QuerySource because we know we don't already have a 
                # LIMIT clause, and can therefore get away with adding one directly rather than 
                # using a subquery
                self.query.limit(self.page_size).offset(self._current_page_offset),
            self.params).fetchall()
            if result:
                self._current_page_offset += self.page_size
                self._iter = iter(result)
                return True
            return False


class QuerySink(Sink):
    """
    Sink to insert of update using the specified query.
    """
    def __init__(self, engine:Engine, query:Union[str, Executable], *, buffer_size:int = 100):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param query: The query to use to insert or update data
        :param buffer_size: The number of results to hold in memory before inserting into the database
        """
        super().__init__()
        self.engine = engine
        if isinstance(query, str):
            query = text(query)
        self.query = query
        self.buffer_size = buffer_size
        self._buffer = []

    def flush(self):
        """
        Flush the buffer to the database
        """
        if not self._buffer:
            return
        with self.engine.begin() as conn:
            conn.execute(self.query, self._buffer)
        self._buffer = []
    
    def get(self):
        row = super().get()
        if self.buffer_size > 1:
            self._buffer.append(row)
            if len(self._buffer) >= self.buffer_size:
                self.flush()
        else:
            with self.engine.begin() as conn:
                conn.execute(self.query, row)
        return row
    
    def close(self):
        self.flush()
        super().close()


class TableInsertSink(QuerySink):
    """
    Sink to insert into a specific table. This should be much more performant than `TableSink`,
    but has fewer features.
    """
    def __init__(self, engine:Engine, table:Union[Table,str]):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param table: The table or view to pull data from
        """
        super().__init__(engine, insert(make_table(engine, table)))


class TableUpdateSink(QuerySink):
    """
    Update-only sink for when you know the relevant items already exist in the database. This
    has more database round-trips than `TableInsertSink`, but still fewer than `TableSink`.
    
    """
    def __init__(self, engine:Engine, table:Union[Table,str], key_columns:Union[Column,str,Sequence[Column],Sequence[str]]=None, default_update_action:UpdateAction=UpdateAction.coalesce, update_actions:Mapping[str,UpdateAction]={}):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param table: The table or view to pull data from
        :param key_column: The column to use as a key in the update clause. Should be unique.
            If not provided, the primary key will be used.
        :param default_update_action: Default value to use when none is found in `update_actions`.
        :param update_actions: Mapping of column names to actions
        """
        table = make_table(engine, table)
        if key_columns is not None:
            columns = make_columns(table, key_columns)
        else:
            pk = table.primary_key
            if pk is None:
                raise ValueError('Table has no primary key; you must specify key_column')
            columns = pk.columns
        self.table = table
        self.update_actions = update_actions
        self.default_update_action = default_update_action
        super().__init__(engine, update(table).where(*[column == bindparam(column.name) for column in columns]))
            
    
    @property
    def query(self):
        """Return the query, with the values bound. (Only the "raw" query is stored, initially)"""
        return self._query.values({
            key:UpdateAction(self.update_actions.get(key, self.default_update_action)).func(
                make_column(self.table, key), 
                bindparam(key),
            ) for key in self.keys()
        })
    
    @query.setter
    def query(self, value):
        self._query = value


class TableSink(Sink):
    """
    General sink for tables that can check if an item exists and intelligently merge items. Most use 
    cases should be satisfied by the more efficient `TableInsertSink`, or `TableUpdateSink`; this 
    class makes two database round-trips per row.

    More efficient "upsert" sinks can also be found, but they are dialect-specific, e.g. `MySQLTableInsertSink`.
    """
    def __init__(self, engine:Engine, table:Union[Table,str], key_columns:Union[Column,str,Sequence[Column],Sequence[str]]=None, *, do_updates=True, update_actions:Mapping[str,UpdateAction]={}, default_update_action:UpdateAction=UpdateAction.coalesce):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param table: The table or view to pull data from
        :param key_column: The column to use as a key in the update clause. Should be unique.
            If not provided, the primary key will be used.
        :param default_update_action: Default value to use when none is found in `update_actions`.
        :param update_actions: Mapping of column names to actions
        """
        super().__init__()
        self.engine = engine
        self.table = table
        if key_columns is not None:
            key_cols = make_columns(self.table, key_columns)
        else:
            pk = table.primary_key
            if pk is None:
                raise ValueError('Table has no primary key; you must specify key_column')
            key_cols = pk.columns
        self.match_condition = [column == bindparam(column.name) for column in key_cols]
        self.key_columns = key_cols
        self.do_updates = do_updates
        self.update_actions = update_actions
        self.default_update_action = default_update_action

    @property
    def query_select(self):
        return select(func.count("*")).select_from(self.table).where(*self.match_condition)
    
    @property
    def query_insert(self):
        return insert(self.table)
    
    @property
    def query_update(self):
        return update(self.table).where(*self.match_condition).values(
            {key:UpdateAction(self.update_actions.get(key, self.default_update_action)).func(
                make_column(self.table, key), 
                bindparam(key), 
            ) for key in self.keys()}
        )


    def get(self):
        row = super().get()
        with self.engine.begin() as conn:
            selected = conn.execute(self.query_select, {col.name:row[col.name] for col in self.key_columns}).scalar()
            if selected and self.do_updates:
                # An existing value was found and we want to update it
                conn.execute(self.query_update, row)
            elif not selected:
                # No existing value was found
                conn.execute(self.query_insert, row)
        return row


class LookupQuery(Lookup):
    """
    Pipeline item to look up a value in a table. This will fetch and store the entire lookup
    table in memory, so should only be used for relatively small lookup tables.
    """
    def __init__(self, engine:Engine, query:Union[str, Executable], *, convert_keys=str):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param query: The query to run in order to build the lookup table. It should return two columns.
            The first is the key and the second is the value.
        :param convert_keys: If provided, a callable that will be used to convert all lookup keys before
            use. May be useful when typing is inconsistent or more flexible typing is needed.Set to None
            to do no conversion.
        """
        if isinstance(query, str):
            query = text(query)
        with engine.begin() as conn:
            result = conn.execute(query).fetchall()
        super().__init__(dict(result), convert_keys=convert_keys)


class LookupTable(Lookup):
    """
    Pipeline item to look up a value in a table. This will fetch and store the entire lookup
    table in memory, so should only be used for relatively small lookup tables.
    """
    def __init__(self, engine:Engine, table:Union[Table,str], key_column:Union[Column,str], value_column:Union[Column,str], *, convert_keys=str):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param table: The table or view to pull data from
        :param key_column: The column that will be the key of the lookup table (must be unique)
        :param value_column: The column that will be the value of the lookup table
        :param convert_keys: If provided, a callable that will be used to convert all lookup keys before
            use. May be useful when typing is inconsistent or more flexible typing is needed.Set to None
            to do no conversion.
        """
        table = make_table(engine, table)
        key_column = make_column(table, key_column)
        value_column = make_column(table, value_column)
        with engine.begin() as conn:
            result = conn.execute(select(key_column, value_column)).fetchall()
        super().__init__(dict(result), convert_keys=convert_keys)


class CollectQuery(CollectDict):
    """
    Run a query and return the results. Allows you to put multiple values for complex queries.

    Example::

        with CollectQuery(engine, "SELECT * FROM the_table WHERE thing1 = :thing1 OR thing2 = :thing2") as query:
            source.take('thing1') >> query.put('thing1')
            source.take('thing2') >> query.put('thing2')
            query.take(0).take('thing3') >> sink.put('thing3')
    """
    _value = None
    def __init__(self, engine:Engine, query:Union[str, Executable]):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param query: The query to execute. You may use ":name" placeholders for the put values.
        """
        if isinstance(query, str):
            query = text(query)
        self.query = query
        self.engine = engine

    def get(self):
        if self._value is None:
            params = super().get()
            with self.engine.begin() as conn:
                self._process_result(conn.execute(self.query, params))
        return self._value
    
    def _process_result(self, result):
        self._value = result.fetchall()
    

class Query(PipelineItem):
    """
    Run a query and return the results, using the received in the pipeline value as a query parameter.
    """
    def __init__(self, engine:Engine, query:Union[str, Executable]):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param query: The query to execute. Use ":value" as a placeholder for whatever value this
            pipeline item receives.
        """
        if isinstance(query, str):
            query = text(query)
        self.query = query
        self.engine = engine

    @lru_cache(32)
    def process(self, value):
        with self.engine.begin() as conn:
            result = conn.execute(self.query, {'value':value})
            return result.fetchall() or None
    

class QueryValue(Query):
    """
    Run a query and return a single value from it. 
    """

    @lru_cache(32)
    def process(self, value):
        with self.engine.begin() as conn:
            result = conn.execute(self.query, {'value':value})
            return result.scalar_one_or_none()


class CollectQueryValue(CollectQuery):
    """
    Run a query and return the results. Allows you to put multiple values for complex queries.

    Example::

        with CollectQueryValue(engine, "SELECT thing3 FROM the_table WHERE thing1 = :thing1 OR thing2 = :thing2 LIMIT 1") as query:
            source.take('thing1') >> query.put('thing1')
            source.take('thing2') >> query.put('thing2')
            query >> sink.put('thing3')
    """
    def _process_result(self, result):
        self._value = result.scalar_one_or_none()


class FetchValue(QueryValue):
    """
    Pipeline item to look up a value in a table. Keeps a small LRU cache of results, but
    otherwise looks up values on-the-fly rather than fetching all at once and keeping
    the entire lookup in memory. This is useful for larger lookup tables.
    """
    def __init__(self, engine:Engine, table:Union[Table,str], key_column:Union[Column,str], value_column:Union[Column,str]):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param table: The table or view to pull data from
        :param key_column: The column that will be the key of the lookup table (must be unique)
        :param value_column: The column that will be the value of the lookup table
        """
        super().__init__(engine, select(make_column(table, value_column)).where(make_column(table, key_column) == bindparam('value')).limit(1))


class QueryRow(Query):
    """
    Run a query and return a single row from it. 
    """

    @lru_cache(32)
    def process(self, value):
        with self.engine.begin() as conn:
            result = conn.execute(self.query, {'value':value})
            return result.one_or_none()
    
    def take(self, key, safe=False) -> Source:
        return super().take_attr(key, safe)
        

class CollectQueryRow(CollectQuery):
    """
    Run a query and return the results. Allows you to put multiple values for complex queries.

    Example::

        with CollectQueryRow(engine, "SELECT * FROM the_table WHERE thing1 = :thing1 OR thing2 = :thing2 LIMIT 1") as query:
            source.take('thing1') >> query.put('thing1')
            source.take('thing2') >> query.put('thing2')
            query.take('thing3') >> sink.put('thing3')
    """
    def _process_result(self, result):
        self._value = result.one_or_none()
    
    def take(self, key, safe=False) -> Source:
        return super().take_attr(key, safe)


class FetchRow(QueryRow):
    """
    Pipeline item to fetch a row by ID, with some caching.
    """
    def __init__(self, engine:Engine, table:Union[Table,str], key_column:Union[Column,str]):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param table: The table or view to pull data from
        :param key_column: The column that will be the key of the lookup table (must be unique)
        """
        super().__init__(engine, select(table).where(make_column(table, key_column) == bindparam('value')).limit(1))


class QueryColumn(Query):
    """
    Run a query and return a list of multiple values from it. 
    """

    @lru_cache(32)
    def process(self, value):
        with self.engine.begin() as conn:
            result = conn.execute(self.query, {'value':value})
            return result.scalars().all() or None
    

class CollectQueryColumn(CollectQuery):
    """
    Run a query and return the results. Allows you to put multiple values for complex queries.

    Example::

        with CollectQueryColumn(engine, "SELECT thing3 FROM the_table WHERE thing1 = :thing1 OR thing2 = :thing2") as query:
            source.take('thing1') >> query.put('thing1')
            source.take('thing2') >> query.put('thing2')
            query >> JoinDelimited(',') >> sink.put('thing3')
    """
    def _process_result(self, result):
        self._value = result.scalars().all() or None


class FetchColumn(QueryColumn):
    """
    Pipeline item to fetch a an array of values, with some caching.
    """
    def __init__(self, engine:Engine, table:Union[Table,str], key_column:Union[Column,str], value_column:Union[Column,str]):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param table: The table or view to pull data from
        :param key_column: The column that will be the key of the lookup table
        :param value_column: The column that will be the value of the lookup table
        """
        super().__init__(engine, select(make_column(table, value_column)).where(make_column(table, key_column) == bindparam('value')))