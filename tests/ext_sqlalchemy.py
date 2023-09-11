import unittest
import sys, os
sys.path.insert(0, os.path.realpath(os.path.join(os.path.basename(__file__), '../')))
from micdrop import *
from micdrop.sink import *
from micdrop.exceptions import StopProcessing as StopProcessingException
from micdrop.ext.sql_alchemy import *
from sqlalchemy import *


class TestSqlAlchemy(unittest.TestCase):
    test_people = [
        {'f_name':'Robert', 'l_name':'Jordan', 'occupation':'Author', 'race':'Human'},
        {'f_name':'J.R.R.', 'l_name':'Tolkien', 'occupation':'Author', 'race':'Human'},
        {'f_name':'Toby', 'l_name':'Mcguire', 'occupation':'Actor', 'race':'Human'},
        {'f_name':'Bilbo', 'l_name':'Baggins', 'occupation':'Burglar', 'race':'Hobbit'},
        {'f_name':'Perrin', 'l_name':'Aybara', 'occupation':'Blacksmith', 'race':'Human'},
        {'f_name':'Peter', 'l_name':'Parker', 'occupation':'Photographer', 'race':'Human (Mutant)'},
    ]
    def setUp(self):
        self.engine = create_engine('sqlite://')
        self.meta = MetaData()
        self.people = Table('people', self.meta,
            Column('id', Integer, primary_key=True),
            Column('f_name', String(255)),
            Column('m_name', String(255)),
            Column('l_name', String(255)),
            Column('dob', Date),
            Column('race', String(255)),
            Column('occupation', String(255)),
        )
        self.books = Table('books', self.meta,
            Column('id', Integer, primary_key=True),
            Column('title', String(255)),
            Column('author', Integer, ForeignKey(self.people.c.id)),
        )
        self.movies = Table('movies', self.meta,
            Column('id', Integer, primary_key=True),
            Column('title', String(255)),
            Column('author', Integer, ForeignKey(self.people.c.id)),
        )
        self.book_character = Table('book_character', self.meta,
            Column('character', Integer, ForeignKey(self.people.c.id)),
            Column('book', Integer, ForeignKey(self.books.c.id)),
        )
        self.movie_character = Table('movie_character', self.meta,
            Column('character', Integer, ForeignKey(self.people.c.id)),
            Column('actor', Integer, ForeignKey(self.people.c.id)),
            Column('movie', Integer, ForeignKey(self.movies.c.id)),
        )
        self.meta.create_all(self.engine)
    
    def populate(self):
        with self.engine.begin() as conn:
            conn.execute(insert(self.people), [
                {'id': 1, 'f_name':'Robert', 'l_name':'Jordan', 'occupation':'Author', 'race':'Human'},
                {'id': 2, 'f_name':'J.R.R.', 'l_name':'Tolkien', 'occupation':'Author', 'race':'Human'},
                {'id': 3, 'f_name':'Toby', 'l_name':'Mcguire', 'occupation':'Actor', 'race':'Human'},
                {'id': 4, 'f_name':'Bilbo', 'l_name':'Baggins', 'occupation':'Burglar', 'race':'Hobbit'},
                {'id': 5, 'f_name':'Perrin', 'l_name':'Aybara', 'occupation':'Blacksmith', 'race':'Human'},
                {'id': 6, 'f_name':'Peter', 'l_name':'Parker', 'occupation':'Photographer', 'race':'Human (Mutant)'},
            ])
            conn.execute(insert(self.books), [
                {'id':1, 'title':'The Eye of the World', 'author':1},
                {'id':2, 'title':'The Hobbit', 'author':2},
                {'id':3, 'title':'The Lord of the Rings', 'author':2},
            ])
            conn.execute(insert(self.book_character), [
                {'book':1, 'character':5},
                {'book':2, 'character':4},
                {'book':3, 'character':4},
            ])
            conn.execute(insert(self.movies), [
                {'id':1, 'title':'Spider-Man 1'},
                {'id':2, 'title':'Spider-Man 2'},
                {'id':3, 'title':'Spider-Man 3'},
            ])
            conn.execute(insert(self.movie_character), [
                {'movie':1, 'character':6, 'actor':3},
                {'movie':2, 'character':6, 'actor':3},
                {'movie':3, 'character':6, 'actor':3},
            ])

    def tearDown(self):
        self.engine = None
        self.meta = None
    
    def test_query_source(self):
        self.populate()
        source = QuerySource(self.engine, select(self.people).where(self.people.c.race == 'Human'))
        sink = DictsSink()
        source.take('f_name') >> sink.put('f_name')
        source.take('l_name') >> sink.put('l_name')
        source.take('occupation') >> sink.put('occupation')
        results = sink.process_all(source, True)
        self.assertEqual(results, [
            {'f_name':'Robert', 'l_name':'Jordan', 'occupation':'Author'},
            {'f_name':'J.R.R.', 'l_name':'Tolkien', 'occupation':'Author'},
            {'f_name':'Toby', 'l_name':'Mcguire', 'occupation':'Actor'},
            {'f_name':'Perrin', 'l_name':'Aybara', 'occupation':'Blacksmith'},
        ])

    def test_table_source(self):
        self.populate()
        source = TableSource(self.engine, self.people)
        sink = DictsSink()
        source.take('f_name') >> sink.put('f_name')
        source.take('l_name') >> sink.put('l_name')
        source.take('occupation') >> sink.put('occupation')
        source.take('race') >> sink.put('race')
        results = sink.process_all(source, True)
        self.assertEqual(results, self.test_people)
    
    def test_query_sink(self):
        self.populate()
        source = IterableSource([
            {'old':'Human','new':'Manling'},
            {'old':'Hobbit','new':'Halfling'},
        ])
        sink = QuerySink(self.engine, update(self.people).values(race=bindparam('new')).where(self.people.c.race == bindparam('old')))
        source.take('old') >> sink.put('old')
        source.take('new') >> sink.put('new')
        results = sink.process_all(source)
        with self.engine.connect() as conn:
            result = conn.execute(select(self.people.c.f_name, self.people.c.l_name, self.people.c.occupation, self.people.c.race))
            self.assertEqual([r._mapping for r in result.all()], [
                {'f_name':'Robert', 'l_name':'Jordan', 'occupation':'Author', 'race':'Manling'},
                {'f_name':'J.R.R.', 'l_name':'Tolkien', 'occupation':'Author', 'race':'Manling'},
                {'f_name':'Toby', 'l_name':'Mcguire', 'occupation':'Actor', 'race':'Manling'},
                {'f_name':'Bilbo', 'l_name':'Baggins', 'occupation':'Burglar', 'race':'Halfling'},
                {'f_name':'Perrin', 'l_name':'Aybara', 'occupation':'Blacksmith', 'race':'Manling'},
                {'f_name':'Peter', 'l_name':'Parker', 'occupation':'Photographer', 'race':'Human (Mutant)'},
            ])

    def test_table_insert_sink(self):
        source = IterableSource(self.test_people)
        sink = TableInsertSink(self.engine, self.people)
        source.take('f_name') >> sink.put('f_name')
        source.take('l_name') >> sink.put('l_name')
        source.take('occupation') >> sink.put('occupation')
        source.take('race') >> sink.put('race')
        sink.process_all(source)
        with self.engine.connect() as conn:
            result = conn.execute(select(self.people.c.f_name, self.people.c.l_name, self.people.c.occupation, self.people.c.race))
            self.assertEqual([r._mapping for r in result.all()], self.test_people)
    
    def test_table_update_sink(self):
        self.populate()
        source = IterableSource([
            {'id': 4, 'occupation':'Retired'},
            {'id': 5, 'occupation':'Dreamwalker'},
            {'id': 6, 'occupation':'Friendly Neighborhood Spider-Man'},
        ])
        sink = TableUpdateSink(self.engine, self.people)
        source.take('id') >> sink.put('id')
        source.take('occupation') >> sink.put('occupation')
        sink.process_all(source)
        with self.engine.connect() as conn:
            result = conn.execute(select(self.people.c.f_name, self.people.c.l_name, self.people.c.occupation, self.people.c.race))
            self.assertEqual([r._mapping for r in result.all()], [
                {'f_name':'Robert', 'l_name':'Jordan', 'occupation':'Author', 'race':'Human'},
                {'f_name':'J.R.R.', 'l_name':'Tolkien', 'occupation':'Author', 'race':'Human'},
                {'f_name':'Toby', 'l_name':'Mcguire', 'occupation':'Actor', 'race':'Human'},
                {'f_name':'Bilbo', 'l_name':'Baggins', 'occupation':'Retired', 'race':'Hobbit'},
                {'f_name':'Perrin', 'l_name':'Aybara', 'occupation':'Dreamwalker', 'race':'Human'},
                {'f_name':'Peter', 'l_name':'Parker', 'occupation':'Friendly Neighborhood Spider-Man', 'race':'Human (Mutant)'},
            ])
    
    def test_table_sink(self):
        pass # TODO

    def test_lookup_query(self):
        pass # TODO

    def test_lookup_table(self):
        self.populate()
        source = TableSource(self.engine, self.books)
        sink = DictsSink()
        source.take('title') >> sink.put('title')
        source.take('author') >> \
            LookupTable(self.engine, self.people, self.people.c.id, self.people.c.l_name) >> \
            sink.put('author')
        results = sink.process_all(source, True)
        self.assertEqual(results, [
            {'title':'The Eye of the World', 'author':'Jordan'},
            {'title':'The Hobbit', 'author':'Tolkien'},
            {'title':'The Lord of the Rings', 'author':'Tolkien'},
        ])

    # TODO test remaining classes