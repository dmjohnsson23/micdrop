import unittest
import sys, os
sys.path.insert(0, os.path.realpath(os.path.join(os.path.basename(__file__), '../')))
from micdrop.utils import DeferredOperand, DeferredOperandConstructorValueMeta
from dataclasses import dataclass

class Doer(metaclass=DeferredOperandConstructorValueMeta):
    def __init__(self, callback):
        self.callback = callback

@dataclass
class TestData:
    number:int
    string:str
    assoc:dict = None
    
class TestDeferredOperand(unittest.TestCase):
    def test_basic_operations(self):
        d = DeferredOperand()
        is_good = d == 'good'
        self.assertFalse(is_good('bad'))
        self.assertTrue(is_good('good'))
        d = DeferredOperand()
        greater_good = d > 'good'
        self.assertFalse(greater_good('exercise'))
        self.assertTrue(greater_good('ice cream'))
    
    def test_call(self):
        d = DeferredOperand()
        returns_home = d() == 'home'
        self.assertFalse(returns_home(lambda: 'dog'))
        self.assertTrue(returns_home(lambda: 'home')) # Home always returns home
    
    def test_getitem(self):
        c = {'my':1, 'very':0, 'excellent':5, 'mother':1, 'just':0, 'served':3, 'us':12, 'nine':1, 'pizzas':9}
        d = DeferredOperand()
        one_mother = d['mother'] == 1
        d = DeferredOperand()
        one_of_us = d['us'] == 1
        self.assertTrue(one_mother(c))
        self.assertFalse(one_of_us(c))

    def test_getattr(self):
        t1 = TestData(3, 'kings')
        t2 = TestData(1, 'little drummer boy')
        t3 = TestData(5, 'golden rings')
        d = DeferredOperand()
        smeagol = d.string == 'golden rings'
        d = DeferredOperand()
        we_only_has_nine = d.number == 9
        self.assertFalse(we_only_has_nine(t1))
        self.assertTrue(smeagol(t3))
        self.assertFalse(smeagol(t2))

    def test_nullsafe(self):
        d = DeferredOperand()
        a_dozen = d.nullsafe == 12
        self.assertFalse(a_dozen(13))
        self.assertTrue(a_dozen(12))
        self.assertIsNone(a_dozen(None))

    def test_return(self):
        t1 = TestData(3, 'kings')
        t2 = TestData(1, 'little drummer boy')
        t3 = TestData(5, 'golden rings')
        d = DeferredOperand()
        get_number = d.number.return_
        d = DeferredOperand()
        get_string = d.string.return_
        self.assertEqual(get_number(t1), 3)
        self.assertEqual(get_string(t1), 'kings')
        self.assertEqual(get_number(t2), 1)
        self.assertEqual(get_string(t2), 'little drummer boy')
        self.assertEqual(get_number(t3), 5)
        self.assertEqual(get_string(t3), 'golden rings')
    
    def test_chaining(self):
        c = {'my':1, 'very':0, 'excellent':5, 'mother':1, 'just':0, 'served':3, 'us':12, 'nine':1, 'pizzas':9}
        d = DeferredOperand()
        has_pizza = d.get('pizzas', 0) > 0
        d = DeferredOperand()
        has_nachos = d.get('nachos', 0) > 0
        self.assertTrue(has_pizza(c)) #plutoisaplanet
        self.assertFalse(has_nachos(c))
        d = DeferredOperand()
        has_pizza = d.nullsafe['pizzas'] > 0
        d = DeferredOperand()
        has_nachos = d.nullsafe['nachos'] > 0
        self.assertTrue(has_pizza(c)) 
        self.assertIsNone(has_nachos(c))
        
        

        
        