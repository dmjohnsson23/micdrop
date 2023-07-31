"""
Extension integrating with the mysql-connector-python library. `https://dev.mysql.com/doc/connector-python/en/`_

Be aware that this library assumes the use of trusted input and therefore does not attempt to 
guard against SQL injection, other than "accidental" injection such as using a keyword as a
table or column name. (The actual values inserted by the various sinks are parameterized and
therefore safe from injection; this note refers mainly to column and table names.)
"""
from ..source import Source
from ..sink import Sink
from typing import Union

class MySqlSource(Source):
    """
    Source to pull from a specific MySql table
    """
    def __init__(self, db_cursor, query, params):
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
        super().__init__(db_cursor, f"SELECT * FROM `{db_name}`.`{table_name}`")


class MySqlTableInsertSink(Sink):
    """
    Source to insert into a specific table. This should be much more performant than 
    `MySqlTableSink`, but has fewer features.
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
            buffer.append(row)
            if len(buffer) >= buffer_size:
                self.db_cursor.executemany(query, buffer)
                buffer = []
        # Send any remaining buffer items
        if buffer:
            self.db_cursor.executemany(query, buffer)

