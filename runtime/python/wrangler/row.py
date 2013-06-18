from transform import Transform
import re
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
				

class Equals(Transform):
	def __init__(self):
		super(Equals,self).__init__()
		self.name = 'equals'

	def test(self, table, row):
		return table[self['lhs']][row] == self['value']

class StartsWith(object):
	"""docstring for StartsWith"""
	def __init__(self, arg):
		super(StartsWith, self).__init__()
		self.name = 'starts_with'

	def test(self, table, row):
		return re.match(str(table[self['lhs']]), self['value'])
		