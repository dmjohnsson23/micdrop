"""
Extension integrating with the SQLAlchemy library. `https://docs.sqlalchemy.org/en/20/`_

You should familiarize yourself with SQLAlchemy Core, as this module does not attempt to abstract
away the details. All the constructs in this module will require you to pass the `sqlalchemy.Engine`
object, and either an `sqlalchemy.Table` or an `sqlalchemy.Executable` to fetch the data from.
"""
from .common import *