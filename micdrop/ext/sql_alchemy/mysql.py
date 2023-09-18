from sqlalchemy import *
from sqlalchemy.dialects.mysql import insert
from typing import Union
from .common import *

__all__ = ('MySQLTableInsertSink',)

class MySQLTableInsertSink(QuerySink):
    """
    Special table insert sink that can take advantage of the MySQL-specific ``ON DUPLICATE KEY UPDATE`` clause.
    """
    def __init__(self, engine:Engine, table:Union[Table,str], *, on_duplicate_key_update=False, default_update_action="COALESCE", update_actions:dict={}):
        """
        :param engine: An SQLAlchemy Engine object to connect to the database
        :param table: The table or view to pull data from
        :param key_column: The column to use as a key in the update clause. Should be unique.
            If not provided, the primary key will be used.
        :param default_update_action: Default value to use when none is found in `update_actions`.
        :param update_actions: Mapping of column names to actions; see `make_value_func`.
        """
        self.table = make_table(engine, table)
        self.update_actions = update_actions
        self.default_update_action = default_update_action
        self.on_duplicate_key_update = on_duplicate_key_update
        super().__init__(engine, insert(self.table))
    
    @property
    def query(self):
        """Return the query, with the values bound. (Only the "raw" query is stored, initially)"""
        query = self._query.values(
            {key:bindparam(key) for key in self.keys()}
        )
        if self.on_duplicate_key_update:
            return query.on_duplicate_key_update({
                key:make_value_func(
                    make_column(self.table, key), 
                    query.inserted[key],
                    self.update_actions.get(key, self.default_update_action)
                ) for key in self.keys()
            })
        else:
            return query
    
    @query.setter
    def query(self, value):
        self._query = value