from transform import Transform
import re, math
from dateutil import parser
class Row(Transform):
	def __init__(self):
		super(Row,self).__init__()
		self['conditions'] = []
		self.name = 'row'

	def test(self, tables, row):
		conditions = self['conditions']

		for cond in conditions:
			if(not cond.test(tables, row)):
				return False
		
		return True


class RowIndex(Transform):
	def __init__(self):
		super(RowIndex,self).__init__()
		self['indices'] = []
		self.name = 'row_index'

	def test(self, tables, row):
		return row in self['indices']

class Empty(Transform):
	def __init__(self):
		super(Empty,self).__init__()
		self.name = 'empty'

	def test(self, table, row):
		for c in table:
			v = c[row]
			if(not v == None and len(str(v))):
				return False
		return True

class IsNull(Transform):
	def __init__(self):
		super(IsNull,self).__init__()
		self.name = 'is_null'

	def test(self, table, row):
		x = table[self['lcol']][row]
		return x == None or len(str(x)) == 0	


class IsType(Transform):
	def __init__(self):
		super(IsType,self).__init__()
		self.name = 'is_type'

	def test(self, table, row):
		return self['type'].parse(table[self['lcol']][row]) == None

class Type(Transform):
	def __init__(self):
		super(Type,self).__init__()
		self.name = 'type'

	def transform(self, values):
		return [self.parse(v) for v in values]

	def compare(self,a,b):
		a = self.parse(a)
		b = self.parse(b)
		if(a < b):
			return -1
		if(a==b):
			return 0
		if(a > b):
			return 1


class Number(Type):
	def __init__(self):
		super(Number,self).__init__()
		self.name = 'number'

	def parse(self, v):
		try:
			return float(v)
		except ValueError:
			return None

class Integer(Type):
	def __init__(self):
		super(Integer,self).__init__()
		self.name = 'integer'


	def parse(self, v):
		try:
			x = float(v)
			return x == math.floor(x)
		except ValueError:
			return None

class String(Type):
	def __init__(self):
		super(String,self).__init__()
		self.name = 'string'


	def parse(self, v):
		return v

class Date(Type):
	def __init__(self):
		super(String,self).__init__()
		self.name = 'string'


	def parse(self, v):
		try:
			return parser.parse(v)
		except ValueError:
			return None				

class Equals(Transform):
	def __init__(self):
		super(Equals,self).__init__()
		self.name = 'equals'

	def test(self, table, row):
		return table[self['lcol']][row] == self['value']


class Contains(Transform):
	"""docstring for Contains"""
	def __init__(self):
		super(Contains, self).__init__()
		self.name = 'contains'

	def test(self, table, row):
		return re.search(self['value'], str(table[self['lcol']][row])) 


class StartsWith(Transform):
	"""docstring for StartsWith"""
	def __init__(self):
		super(StartsWith, self).__init__()
		self.name = 'starts_with'

	def test(self, table, row):
		return re.match(self['value'], str(table[self['lcol']][row]))
		