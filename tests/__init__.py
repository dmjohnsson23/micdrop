import unittest
import sys, os
sys.path.insert(0, os.path.realpath(os.path.join(os.path.basename(__file__), '../')))
from micdrop.pipeline import *
from micdrop.sink import *
from micdrop.source import *

class TestPipeline(unittest.TestCase):
    def test_loose(self):
        pipeline = StaticSource(5) >> LooseSink()
        self.assertEqual(pipeline.get(), 5, 'Get static value')
        pipeline.reset()
        self.assertEqual(pipeline.get(), 5, 'Get same static value after reset')
        pipeline = IterableSource(range(5)) >> LooseSink()
        self.assertEqual(pipeline.get(), 0, 'Get iterable value')
        pipeline.reset()
        self.assertEqual(pipeline.get(), 1, 'Get different iterable value after reset')
        expected_value = 9
        @FactorySource
        def factory():
            nonlocal expected_value
            expected_value += 1
            return expected_value
        pipeline = factory >> LooseSink()
        self.assertEqual(pipeline.get(), 10, 'Get factory value')
        pipeline.reset()
        self.assertEqual(pipeline.get(), 11, 'Get different factory value after reset')

    def test_basic_type_conversions(self):
        pipeline = StaticSource('5') >> int >> LooseSink()
        self.assertEqual(pipeline.get(), 5, 'Convert to int')
        pipeline = StaticSource('5.5') >> float >> LooseSink()
        self.assertEqual(pipeline.get(), 5.5, 'Convert to float')
        pipeline = StaticSource(25) >> str >> LooseSink()
        self.assertEqual(pipeline.get(), '25', 'Convert to str')
    
    def test_destructure_dict(self):
        source = StaticSource({'a':1, 'b':2, 'c':3})
        a = source.take('a') >> LooseSink()
        b = source.take('b') >> LooseSink()
        c = source.take('c') >> LooseSink()
        self.assertEqual(a.get(), 1, 'Destructure dict')
        self.assertEqual(b.get(), 2, 'Destructure dict')
        self.assertEqual(c.get(), 3, 'Destructure dict')
    
    def test_destructure_list(self):
        source = StaticSource([1, 2, 3])
        a = source.take(0) >> LooseSink()
        b = source.take(1) >> LooseSink()
        c = source.take(2) >> LooseSink()
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
        StaticSource(1) >> collect.put()
        StaticSource(3) >> collect.put('three')
        StaticSource(2) >> collect.put('two')
        
    
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
            self.assertEqual(choice.get(), 'first')
            choice.reset()
            self.assertEqual(choice.get(), 'second')
            choice.reset()
            self.assertEqual(choice.get(), 'third')
            choice.reset()
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
            self.assertEqual(collect.get(), {'peanut butter':'chocolate','milk':None,'ice cream':None,'chocolate':None})
            collect.reset()
            self.assertEqual(collect.get(), {'peanut butter':None,'milk':'chocolate','ice cream':None,'chocolate':None})
            collect.reset()
            self.assertEqual(collect.get(), {'peanut butter':None,'milk':None,'ice cream':'chocolate','chocolate':None})
            collect.reset()
            self.assertEqual(collect.get(), {'peanut butter':None,'milk':None,'ice cream':None,'chocolate':'chocolate'})

    def test_pipeline_segment(self):
        source1 = IterableSource(range(5))
        source2 = IterableSource(range(4, 0, -1))
        pipeline = PipelineSegment() >> (lambda val: val * 5) >> str
        sink1 = source1 >> pipeline.apply()
        sink2 = source2 >> pipeline.apply() >> float
        self.assertEqual(sink1.get(), '0')
        self.assertEqual(sink2.get(), 20.0)
        sink1.reset()
        sink2.reset()
        self.assertEqual(sink1.get(), '5')
        self.assertEqual(sink2.get(), 15.0)


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
        with DictsSource(self.test_data_1) as source:
            self.assertTrue(source.next(), 'Has next for first iteration')
            self.assertEqual(source.get('f_name'), 'Bilbo')
            self.assertTrue(source.next(), 'Has next for second iteration')
            self.assertEqual(source.get('f_name'), 'Peter')
            self.assertEqual(source.get('l_name'), 'Parker')
            self.assertTrue(source.next(), 'Has next for third iteration')
            self.assertEqual(source.get('occupation'), 'Blacksmith')
            self.assertFalse(source.next(), 'Has no fourth iteration')
    
    def test_dicts_sink(self):
        sink = DictsSink()
        source = DictsSource([{}, {}, {}])

        IterableSource(range(33)) >> sink.put('id')
        StaticSource('Human') >> sink.put('race')

        self.assertEqual(sink.process_all(source, True), [
            {'id':0, 'race':'Human'},
            {'id':1, 'race':'Human'},
            {'id':2, 'race':'Human'},
        ])



    def test_integration(self):
        source = DictsSource(self.test_data_1)
        sink = DictsSink()

        IterableSource(range(1, 99999)) >> sink.put('record number')

        name = CollectList()
        source.take('f_name') >> name.put()
        source.take('m_name') >> name.put()
        source.take('l_name') >> name.put()
        name >> JoinDelimited(' ') >> sink.put('full name')

        source.take('dob') >> ParseDate() >> FormatDate('%m/%d/%Y') >> sink.put('birth date')

        source.take('race') >> sink.put('race')

        self.assertEqual(sink.process_all(source, True), [
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
        source = DictsSource(self.test_data_1)
        sink = MultiSink(
            s1 = DictsSink(),
            s2 = DictsSink()
        )

        source.take('f_name') >> sink.s1.put('f_name')
        source.take('l_name') >> sink.s1.put('l_name')
        source.take('race') >> sink.s2.put('race')
        source.take('occupation') >> sink.s2.put('occupation')

        self.assertEqual(sink.process_all(source, True), [
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