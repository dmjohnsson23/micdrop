import unittest
import sys, os
sys.path.insert(0, os.path.realpath(os.path.join(os.path.basename(__file__), '../')))
from micdrop import *

class TestPipeline(unittest.TestCase):
    def test_loose(self):
        pipeline = StaticSource(5) >> Put()
        pipeline.idempotent_next(0)
        self.assertEqual(pipeline.get(), 5, 'Get static value')
        pipeline.idempotent_next(1)
        self.assertEqual(pipeline.get(), 5, 'Get same static value after next')
        pipeline = IterableSource(range(5)) >> Put()
        pipeline.idempotent_next(0)
        self.assertEqual(pipeline.get(), 0, 'Get iterable value')
        pipeline.idempotent_next(1)
        self.assertEqual(pipeline.get(), 1, 'Get different iterable value after next')
        expected_value = 9
        @FactorySource
        def factory():
            nonlocal expected_value
            expected_value += 1
            return expected_value
        pipeline = factory >> Put()
        pipeline.idempotent_next(0)
        self.assertEqual(pipeline.get(), 10, 'Get factory value')
        pipeline.idempotent_next(1)
        self.assertEqual(pipeline.get(), 11, 'Get different factory value after next')

    def test_basic_type_conversions(self):
        pipeline = StaticSource('5') >> int >> Put()
        self.assertEqual(pipeline.get(), 5, 'Convert to int')
        pipeline = StaticSource('5.5') >> float >> Put()
        self.assertEqual(pipeline.get(), 5.5, 'Convert to float')
        pipeline = StaticSource(25) >> str >> Put()
        self.assertEqual(pipeline.get(), '25', 'Convert to str')
    
    def test_destructure_dict(self):
        source = StaticSource({'a':1, 'b':2, 'c':3})
        a = source.take('a') >> Put()
        b = source.take('b') >> Put()
        c = source.take('c') >> Put()
        self.assertEqual(a.get(), 1, 'Destructure dict')
        self.assertEqual(b.get(), 2, 'Destructure dict')
        self.assertEqual(c.get(), 3, 'Destructure dict')
    
    def test_destructure_list(self):
        source = StaticSource([1, 2, 3])
        a = source.take(0) >> Put()
        b = source.take(1) >> Put()
        c = source.take(2) >> Put()
        self.assertEqual(a.get(), 1, 'Destructure list')
        self.assertEqual(b.get(), 2, 'Destructure list')
        self.assertEqual(c.get(), 3, 'Destructure list')
    
    def test_collect_dict(self):
        collect = CollectDict()
        StaticSource(1) >> collect.put('a')
        StaticSource(2) >> collect.put('b')
        StaticSource(3) >> collect.put('c')
        self.assertEqual({'a':1, 'b':2, 'c':3}, collect.get())

    def test_collect_list(self):
        collect = CollectList()
        StaticSource(1) >> collect.put()
        StaticSource(2) >> collect.put()
        StaticSource(3) >> collect.put()
        self.assertEqual([1, 2, 3], collect.get())
    
    def test_collect_format_string(self):
        collect = CollectFormatString("Sometimes you think you're #{}, when in reality you're more of a #{other}.")
        StaticSource(1) >> collect.put()
        StaticSource(2) >> collect.put('other')
        self.assertEqual("Sometimes you think you're #1, when in reality you're more of a #2.", collect.get())
    
    def test_collect_call(self):
        @CollectCall
        def collect(one, two, three):
            self.assertEqual(one, 1)
            self.assertEqual(two, 2)
            self.assertEqual(three, 3)
            return one + two + three
        StaticSource(1) >> collect.put()
        StaticSource(3) >> collect.put('three')
        StaticSource(2) >> collect.put('two')
        self.assertEqual(6, collect.get())
    
    def test_list_join_split(self):
        from functools import partial
        split = StaticSource('1,2,3') >> SplitDelimited(',') >> partial(map, int) >> list
        self.assertEqual([1, 2, 3], split.get())
        join = StaticSource([1, 2, 3]) >> JoinDelimited(',')
        self.assertEqual('1,2,3', join.get())

    def test_dict_join_split(self):
        raw = {
            'Thing 1': '1',
            'Thing 2': '2',
        }
        formatted = "Thing 1: 1\nThing 2: 2"
        split = StaticSource(formatted) >> SplitKeyValue(': ')
        self.assertEqual(raw, split.get())
        join = StaticSource(raw) >> JoinKeyValue(': ')
        self.assertEqual(formatted, join.get())
    
    def test_date_time(self):
        from datetime import date, datetime
        pipeline = StaticSource('2022-02-22') >> ParseDate()
        self.assertEqual(pipeline.get(), date(2022, 2, 22))
        pipeline = StaticSource('00/00/0000') >> ParseDate('%m/%d/%Y', True)
        self.assertIsNone(pipeline.get())
        pipeline = StaticSource(datetime(2020, 2, 2, 11, 11, 11)) >> FormatDatetime()
        self.assertEqual('2020-02-02 11:11:11', pipeline.get())
    
    def test_choose(self):
        with IterableSource(range(4)) >> Choose() as choice:
            StaticSource('first') >> (choice.value == 0)
            StaticSource('second') >> (choice.value == 1)
            StaticSource('third') >> (choice.value == 2)
            StaticSource('default') >> choice.fallback()
            choice.idempotent_next(0)
            self.assertEqual(choice.get(), 'first')
            choice.idempotent_next(1)
            self.assertEqual(choice.get(), 'second')
            choice.idempotent_next(2)
            self.assertEqual(choice.get(), 'third')
            choice.idempotent_next(3)
            self.assertEqual(choice.get(), 'default')
    
    def test_branch(self):
        with CollectDict() as collect:
            with IterableSource(range(4)) >> Branch() as cases:
                StaticSource('chocolate') >> cases.put()
                with cases.value == 0 as case:
                    case.take() >> collect.put('peanut butter')
                with cases.value == 1 as case:
                    case.take() >> collect.put('milk')
                with cases.value == 2 as case:
                    case.take() >> collect.put('ice cream')
                with cases.fallback() as case:
                    case.take() >> collect.put('chocolate')
            collect.idempotent_next(0)
            self.assertEqual(collect.get(), {'peanut butter':'chocolate','milk':None,'ice cream':None,'chocolate':None})
            collect.idempotent_next(1)
            self.assertEqual(collect.get(), {'peanut butter':None,'milk':'chocolate','ice cream':None,'chocolate':None})
            collect.idempotent_next(2)
            self.assertEqual(collect.get(), {'peanut butter':None,'milk':None,'ice cream':'chocolate','chocolate':None})
            collect.idempotent_next(3)
            self.assertEqual(collect.get(), {'peanut butter':None,'milk':None,'ice cream':None,'chocolate':'chocolate'})

    def test_pipeline_segment(self):
        source1 = IterableSource(range(5))
        source2 = IterableSource(range(4, 0, -1))
        pipeline = PipelineSegment() >> (lambda val: val * 5) >> str
        sink1 = source1 >> pipeline.apply()
        sink2 = source2 >> pipeline.apply() >> float
        sink1.idempotent_next(0)
        sink2.idempotent_next(0)
        self.assertEqual(sink1.get(), '0')
        self.assertEqual(sink2.get(), 20.0)
        sink1.idempotent_next(1)
        sink2.idempotent_next(1)
        self.assertEqual(sink1.get(), '5')
        self.assertEqual(sink2.get(), 15.0)
    
    def test_foreach(self):
        source = IterableSource([
            {'a':'first', 'b':[1, 2, 3, 4, 5]},
            {'a':'second', 'b':[0, 1, 2, 3]},
            {'a':'third', 'b':[2, 3, 4, 5, 6, 7]},
        ])
        pipe = source.take('b') >> ForEach(PipelineSegment() >> (DeferredOperand() + ord('a')) >> chr >> str.upper) >> JoinDelimited('')
        pipe.idempotent_next(0)
        self.assertEqual(pipe.get(), 'BCDEF')
        pipe.idempotent_next(1)
        self.assertEqual(pipe.get(), 'ABCD')
        pipe.idempotent_next(2)
        self.assertEqual(pipe.get(), 'CDEFGH')

    
    def test_skip_row(self):
        source = IterableSource(range(5))
        with source >> Choose() as choice:
            SkipRow() >> (choice.value == 2)
            source >> choice.fallback()
            choice.idempotent_next(0)
            self.assertEqual(choice.get(), 0)
            choice.idempotent_next(1)
            self.assertEqual(choice.get(), 1)
            choice.idempotent_next(2)
            with self.assertRaises(SkipRowException):
                choice.get()
            choice.idempotent_next(3)
            self.assertEqual(choice.get(), 3)
            choice.idempotent_next(4)
            self.assertEqual(choice.get(), 4)
    
    def test_value_other(self):
        value_source = IterableSource(['Beans', 'Other', 'Meat', 'Cheese', 'Greens'])
        other_source = IterableSource([None, 'Lime Jello', 'Beef', 'Cheddar', None])
        mapping = {
            'Beans': "Frijoles",
            'Meat': "Carne",
            'Cheese': "Queso",
        }
        collect = CollectValueOther()
        value_source >> mapping >> collect.put_mapped()
        value_source >> collect.put_unmapped()
        other_source >> collect.put_other()
        
        collect.idempotent_next(1)
        self.assertEqual(collect.get(), ('Frijoles', None))
        collect.idempotent_next(2)
        self.assertEqual(collect.get(), (None, 'Other: Lime Jello'))
        collect.idempotent_next(3)
        self.assertEqual(collect.get(), ('Carne', 'Beef'))
        collect.idempotent_next(4)
        self.assertEqual(collect.get(), ('Queso', 'Cheddar'))
        collect.idempotent_next(5)
        self.assertEqual(collect.get(), (None, 'Greens'))


class TestSourceSink(unittest.TestCase):
    test_data_1 = [
        {
            'f_name': 'Bilbo',
            'm_name': None,
            'l_name': 'Baggins',
            'dob': None,
            'books': ['The Lord of the Rings', 'The Hobbit'],
            'movies': ['The Fellowship of the Ring'],
            'race': 'Hobbit',
            'occupation': 'Burglar',
            'abilities': ['Riddles'],
        },
        {
            'f_name': 'Peter',
            'm_name': None,
            'l_name': 'Parker',
            'dob': '1972-05-11',
            'books': [],
            'movies': ['Spider-man 1', 'Spider-man 2', 'Spider-man 3'],
            'race': 'Human',
            'occupation': 'Photographer',
            'abilities': ['Climb walls', 'Shoot webs', 'Spidey sense'],
        },
        {
            'f_name': 'Perrin',
            'm_name': None,
            'l_name': 'Aybara',
            'dob': None,
            'books': ['The Eye of the World', 'The Great Hunt', 'The Dragon Reborn'],
            'movies': None,
            'race': 'Human',
            'occupation': 'Blacksmith',
            'abilities': ['Dreamwalker', 'Wolfbrother'],
        }
    ]

    def test_dicts_source(self):
        with IterableSource(self.test_data_1) as source:
            source.idempotent_next(0)
            self.assertEqual(source.get()['f_name'], 'Bilbo')
            source.idempotent_next(1)
            self.assertEqual(source.get()['f_name'], 'Peter')
            self.assertEqual(source.get()['l_name'], 'Parker')
            source.idempotent_next(2)
            self.assertEqual(source.get()['occupation'], 'Blacksmith')
            with self.assertRaises((StopProcessingException, StopIteration)):
                source.idempotent_next(3)
                source.get()
    
    def test_dicts_sink(self):
        sink = DictsSink()
        source = IterableSource(range(3))

        source >> sink.put('id')
        StaticSource('Human') >> sink.put('race')

        self.assertEqual(process_all(sink, True), [
            {'id':0, 'race':'Human'},
            {'id':1, 'race':'Human'},
            {'id':2, 'race':'Human'},
        ])
    
    def test_sink_whole_value(self):
        source = IterableSource(self.test_data_1)
        sink = Sink()

        source >> sink

        self.assertEqual(process_all(sink, True), self.test_data_1)


    def test_integration(self):
        source = IterableSource(self.test_data_1)
        sink = DictsSink()

        IterableSource(range(1, 99999)) >> sink.put('record number')

        name = CollectList()
        source.take('f_name') >> name.put()
        source.take('m_name') >> name.put()
        source.take('l_name') >> name.put()
        name >> JoinDelimited(' ') >> sink.put('full name')

        source.take('dob') >> ParseDate() >> FormatDate('%m/%d/%Y') >> sink.put('birth date')

        source.take('race') >> sink.put('race')

        self.assertEqual(process_all(sink, True), [
            {
                'record number': 1,
                'full name': "Bilbo Baggins",
                'birth date': None,
                'race': 'Hobbit',
            },
            {
                'record number': 2,
                'full name': 'Peter Parker',
                'birth date': '05/11/1972',
                'race': 'Human',
            },
            {
                'record number': 3,
                'full name': 'Perrin Aybara',
                'birth date': None,
                'race': 'Human',
            }
        ])

    def test_multi_sink(self):
        source = IterableSource(self.test_data_1)
        sink = MultiSink(
            s1 = DictsSink(),
            s2 = DictsSink()
        )

        source.take('f_name') >> sink.s1.put('f_name')
        source.take('l_name') >> sink.s1.put('l_name')
        source.take('race') >> sink.s2.put('race')
        source.take('occupation') >> sink.s2.put('occupation')

        self.assertEqual(process_all(sink, True), [
            [
                {'f_name': 'Bilbo', 'l_name': 'Baggins'},
                {'race': 'Hobbit', 'occupation': 'Burglar'},
            ],
            [
                {'f_name': 'Peter', 'l_name': 'Parker'},
                {'race': 'Human', 'occupation': 'Photographer'},
            ],
            [
                {'f_name': 'Perrin', 'l_name': 'Aybara'},
                {'race': 'Human', 'occupation': 'Blacksmith'},
            ],
        ])
    

    def test_skip_row(self):
        source = IterableSource(self.test_data_1)
        sink = DictsSink()

        source.take('f_name') >> sink.put('f_name')
        with source.take('race') >> Choose() as choice:
            source.take('occupation') >> (choice.value == 'Human')
            SkipRow() >> choice.fallback()
            choice >> sink.put('occupation')
        self.assertEqual(process_all(sink, True), [
            {'f_name': 'Peter', 'occupation': 'Photographer'},
            {'f_name': 'Perrin', 'occupation': 'Blacksmith'},
        ])
    
    def test_repeater_source(self):
        from itertools import cycle
        source = RepeaterSource(IterableSource([
            {'id':1, 'h_phone': '1234657890', 'w_phone': None, 'cell': '9876543210'},
            {'id':2, 'h_phone': None, 'w_phone': '7410852963', 'cell': '9638527410'},
        ]))
        sink = DictsSink()

        source.take('id') >> sink.put('id')
        IterableSource(cycle(('Home', 'Work', 'Cell'))) >> sink.put('type')
        source.take_each('h_phone', 'w_phone', 'cell') >> SkipIf.value.is_(None) >> sink.put('number')

        self.assertEqual(process_all(sink, True), [
            {'id':1, 'type': 'Home', 'number': '1234657890'},
            {'id':1, 'type': 'Cell', 'number': '9876543210'},
            {'id':2, 'type': 'Work', 'number': '7410852963'},
            {'id':2, 'type': 'Cell', 'number': '9638527410'},
        ])

    def test_repeater_sink(self):
        source = IterableSource([
            {'id': 1, 'things':['a', 'b', 'c']},
            {'id': 2, 'things':['d', 'e']},
            {'id': 3, 'things':[]},
            {'id': 4, 'things':['f']},
        ])
        sink = RepeaterSink(DictsSink())

        source.take('id') >> sink.put('id')
        source.take('things') >> sink.put_each('thing')

        self.assertEqual(process_all(sink, True), [
            # TODO I'm not sure I like the nested lists, reconsider how this class works
            [{'id':1, 'thing': 'a'},
            {'id':1, 'thing': 'b'},
            {'id':1, 'thing': 'c'}],
            [{'id':2, 'thing': 'd'},
            {'id':2, 'thing': 'e'}],
            [], # TODO not sure what the expected behavior in this case (id 3) should be, revist
            [{'id':4, 'thing': 'f'}],
        ])
